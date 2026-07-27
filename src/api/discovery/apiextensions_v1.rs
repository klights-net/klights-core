use super::*;
pub async fn apiextensions_group() -> Json<APIGroup> {
    Json(APIGroup {
        name: "apiextensions.k8s.io".to_string(),
        versions: vec![GroupVersionForDiscovery {
            group_version: "apiextensions.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        }],
        preferred_version: GroupVersionForDiscovery {
            group_version: "apiextensions.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        },
    })
}

pub async fn scheduling_group() -> Json<APIGroup> {
    Json(APIGroup {
        name: "scheduling.k8s.io".to_string(),
        versions: vec![GroupVersionForDiscovery {
            group_version: "scheduling.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        }],
        preferred_version: GroupVersionForDiscovery {
            group_version: "scheduling.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        },
    })
}

pub async fn node_k8s_io_group() -> Json<APIGroup> {
    Json(APIGroup {
        name: "node.k8s.io".to_string(),
        versions: vec![GroupVersionForDiscovery {
            group_version: "node.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        }],
        preferred_version: GroupVersionForDiscovery {
            group_version: "node.k8s.io/v1".to_string(),
            version: "v1".to_string(),
        },
    })
}

pub async fn apiextensions_v1_resources() -> Json<APIResourceList> {
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
        group_version: "apiextensions.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "customresourcedefinitions".to_string(),
                singular_name: "customresourcedefinition".to_string(),
                namespaced: false,
                kind: "CustomResourceDefinition".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["crd".to_string(), "crds".to_string()]),
                categories: Some(vec!["api-extensions".to_string()]),
            },
            APIResource {
                name: "customresourcedefinitions/status".to_string(),
                singular_name: String::new(),
                namespaced: false,
                kind: "CustomResourceDefinition".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}

pub async fn admissionregistration_v1_resources() -> Json<APIResourceList> {
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
        group_version: "admissionregistration.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "mutatingwebhookconfigurations".to_string(),
                singular_name: "mutatingwebhookconfiguration".to_string(),
                namespaced: false,
                kind: "MutatingWebhookConfiguration".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: Some(vec!["api-extensions".to_string()]),
            },
            APIResource {
                name: "mutatingwebhookconfigurations/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "MutatingWebhookConfiguration".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "validatingwebhookconfigurations".to_string(),
                singular_name: "validatingwebhookconfiguration".to_string(),
                namespaced: false,
                kind: "ValidatingWebhookConfiguration".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: Some(vec!["api-extensions".to_string()]),
            },
            APIResource {
                name: "validatingwebhookconfigurations/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "ValidatingWebhookConfiguration".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "validatingadmissionpolicies".to_string(),
                singular_name: "validatingadmissionpolicy".to_string(),
                namespaced: false,
                kind: "ValidatingAdmissionPolicy".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: Some(vec!["api-extensions".to_string()]),
            },
            APIResource {
                name: "validatingadmissionpolicybindings".to_string(),
                singular_name: "validatingadmissionpolicybinding".to_string(),
                namespaced: false,
                kind: "ValidatingAdmissionPolicyBinding".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: Some(vec!["api-extensions".to_string()]),
            },
            APIResource {
                name: "validatingadmissionpolicies/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "ValidatingAdmissionPolicy".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "validatingadmissionpolicybindings/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "ValidatingAdmissionPolicyBinding".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}

/// Resource list for flowcontrol.apiserver.k8s.io/v1
pub async fn flowcontrol_v1_resources() -> Json<APIResourceList> {
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
        group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "flowschemas".to_string(),
                singular_name: "flowschema".to_string(),
                namespaced: false,
                kind: "FlowSchema".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "flowschemas/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "FlowSchema".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "prioritylevelconfigurations".to_string(),
                singular_name: "prioritylevelconfiguration".to_string(),
                namespaced: false,
                kind: "PriorityLevelConfiguration".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "prioritylevelconfigurations/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "PriorityLevelConfiguration".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}

/// Resource list for apiregistration.k8s.io/v1
pub async fn apiregistration_v1_resources() -> Json<APIResourceList> {
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
        group_version: "apiregistration.k8s.io/v1".to_string(),
        resources: vec![
            APIResource {
                name: "apiservices".to_string(),
                singular_name: "apiservice".to_string(),
                namespaced: false,
                kind: "APIService".to_string(),
                verbs: standard_verbs.clone(),
                short_names: None,
                categories: None,
            },
            APIResource {
                name: "apiservices/status".to_string(),
                singular_name: "".to_string(),
                namespaced: false,
                kind: "APIService".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}

/// Resource list for authentication.k8s.io/v1
pub async fn authentication_v1_resources() -> Json<APIResourceList> {
    Json(APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "authentication.k8s.io/v1".to_string(),
        resources: vec![APIResource {
            name: "tokenreviews".to_string(),
            singular_name: "tokenreview".to_string(),
            namespaced: false,
            kind: "TokenReview".to_string(),
            verbs: vec!["create".to_string()],
            short_names: None,
            categories: None,
        }],
    })
}

pub async fn autoscaling_v1_resources() -> Json<APIResourceList> {
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
        group_version: "autoscaling/v1".to_string(),
        resources: vec![
            APIResource {
                name: "horizontalpodautoscalers".to_string(),
                singular_name: "horizontalpodautoscaler".to_string(),
                namespaced: true,
                kind: "HorizontalPodAutoscaler".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["hpa".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResource {
                name: "horizontalpodautoscalers/status".to_string(),
                singular_name: String::new(),
                namespaced: true,
                kind: "HorizontalPodAutoscaler".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}

pub async fn autoscaling_v2_resources() -> Json<APIResourceList> {
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
        group_version: "autoscaling/v2".to_string(),
        resources: vec![
            APIResource {
                name: "horizontalpodautoscalers".to_string(),
                singular_name: "horizontalpodautoscaler".to_string(),
                namespaced: true,
                kind: "HorizontalPodAutoscaler".to_string(),
                verbs: standard_verbs.clone(),
                short_names: Some(vec!["hpa".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResource {
                name: "horizontalpodautoscalers/status".to_string(),
                singular_name: String::new(),
                namespaced: true,
                kind: "HorizontalPodAutoscaler".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                short_names: None,
                categories: None,
            },
        ],
    })
}
