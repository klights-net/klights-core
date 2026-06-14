use super::*;
/// Returns true if the given comma-separated media-type list contains at least one entry whose
/// semicolon-delimited parameters all match `required`.
///
/// The Accept header from client-go's `downloadAPIs()` is a comma-separated list of preferred
/// media types, e.g.:
///   `application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,application/json;...`
///
/// A naïve split-by-semicolon approach contaminates the last param of the first media type with
/// the leading chars of the next media type (e.g. `as=APIGroupDiscoveryList,application/json`),
/// causing the exact-string match to fail.  This function splits by commas first so each media
/// type is inspected independently.
pub fn accept_has_params(accept: &str, required: &[(&str, &str)]) -> bool {
    accept.split(',').any(|media_type| {
        let params: Vec<&str> = media_type.split(';').collect();
        required.iter().all(|(k, v)| {
            let target = format!("{}={}", k, v);
            params.iter().any(|part| part.trim() == target)
        })
    })
}

/// Performs proper content negotiation: prefers `v=v2`, falls back to `v=v2beta1`.
/// Clients that only request `v=v2beta1` (e.g. older sonobuoy/client-go) get v2beta1 back,
/// not v2, so they can parse the response.
pub fn wants_aggregated_discovery(headers: &HeaderMap) -> Option<&'static str> {
    let accept = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Parse semicolon-separated media-type parameters (order-independent).
    // Accept: application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList
    let has_v2 = accept_has_params(accept, &[("v", "v2"), ("as", "APIGroupDiscoveryList")]);
    let has_v2beta1 =
        accept_has_params(accept, &[("v", "v2beta1"), ("as", "APIGroupDiscoveryList")]);
    if has_v2 {
        Some("v2")
    } else if has_v2beta1 {
        Some("v2beta1")
    } else {
        None
    }
}

pub async fn api_groups(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let mut groups = vec![
        APIGroup {
            name: "apps".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apps/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apps/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "autoscaling".to_string(),
            versions: vec![
                GroupVersionForDiscovery {
                    group_version: "autoscaling/v2".to_string(),
                    version: "v2".to_string(),
                },
                GroupVersionForDiscovery {
                    group_version: "autoscaling/v1".to_string(),
                    version: "v1".to_string(),
                },
            ],
            preferred_version: GroupVersionForDiscovery {
                group_version: "autoscaling/v2".to_string(),
                version: "v2".to_string(),
            },
        },
        APIGroup {
            name: "batch".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "batch/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "batch/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "coordination.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "coordination.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "coordination.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "discovery.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "discovery.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "discovery.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "events.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "events.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "events.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "networking.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "networking.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "networking.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "storage.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "storage.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "node.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "node.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "node.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "policy".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "policy/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "policy/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "rbac.authorization.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "rbac.authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "rbac.authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "authorization.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "authorization.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "certificates.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "certificates.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "certificates.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "apiextensions.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apiextensions.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apiextensions.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "admissionregistration.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "admissionregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "admissionregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "scheduling.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "scheduling.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "scheduling.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "flowcontrol.apiserver.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "flowcontrol.apiserver.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "apiregistration.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "apiregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "apiregistration.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
        APIGroup {
            name: "authentication.k8s.io".to_string(),
            versions: vec![GroupVersionForDiscovery {
                group_version: "authentication.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            }],
            preferred_version: GroupVersionForDiscovery {
                group_version: "authentication.k8s.io/v1".to_string(),
                version: "v1".to_string(),
            },
        },
    ];

    // Dynamically add CRD API groups from the registry
    let crd_versions_by_group = state.crd_registry.list_versions_by_group().await;
    for (group, versions) in crd_versions_by_group {
        // Skip if already in static list
        if groups.iter().any(|g| g.name == group) {
            continue;
        }
        let versions_for_discovery: Vec<GroupVersionForDiscovery> = versions
            .iter()
            .map(|version| GroupVersionForDiscovery {
                group_version: format!("{}/{}", group, version),
                version: version.clone(),
            })
            .collect();
        let preferred_version =
            versions_for_discovery
                .first()
                .cloned()
                .unwrap_or(GroupVersionForDiscovery {
                    group_version: format!("{}/{}", group, versions[0]),
                    version: versions[0].clone(),
                });
        groups.push(APIGroup {
            name: group,
            versions: versions_for_discovery,
            preferred_version,
        });
    }

    if let Ok(api_service_groups) = apiservice_group_versions(&state).await {
        for (group, versions_set) in api_service_groups {
            if groups.iter().any(|g| g.name == group) {
                continue;
            }
            let versions: Vec<String> = versions_set.into_iter().collect();
            if versions.is_empty() {
                continue;
            }
            let versions_for_discovery: Vec<GroupVersionForDiscovery> = versions
                .iter()
                .map(|v| GroupVersionForDiscovery {
                    group_version: format!("{}/{}", group, v),
                    version: v.clone(),
                })
                .collect();
            let preferred =
                versions_for_discovery
                    .first()
                    .cloned()
                    .unwrap_or(GroupVersionForDiscovery {
                        group_version: format!("{}/{}", group, versions[0]),
                        version: versions[0].clone(),
                    });
            groups.push(APIGroup {
                name: group,
                versions: versions_for_discovery,
                preferred_version: preferred,
            });
        }
    }

    if let Some(agg_version) = wants_aggregated_discovery(&headers) {
        let std_verbs = || {
            vec![
                "create".to_string(),
                "delete".to_string(),
                "deletecollection".to_string(),
                "get".to_string(),
                "list".to_string(),
                "patch".to_string(),
                "update".to_string(),
                "watch".to_string(),
            ]
        };

        // /apis returns named API groups only. Core v1 discovery lives at /api.
        let mut items = Vec::new();
        for g in &groups {
            let mut versions = Vec::new();
            for v in &g.versions {
                let mut resources = aggregated_resources_for_group_version(&g.name, &v.version);
                // For CRD groups (empty static resources), populate from CRD registry
                if resources.is_empty() {
                    let crd_resources =
                        state.crd_registry.list_resources(&g.name, &v.version).await;
                    for crd in crd_resources {
                        resources.push(APIResourceDiscovery {
                            resource: crd.plural.clone(),
                            response_kind: APIResourceResponseKind {
                                kind: crd.kind.clone(),
                            },
                            scope: if crd.namespaced {
                                "Namespaced".to_string()
                            } else {
                                "Cluster".to_string()
                            },
                            singular_resource: crd.singular.clone(),
                            verbs: std_verbs(),
                            subresources: Vec::new(),
                            ..Default::default()
                        });
                    }
                }
                if resources.is_empty() {
                    resources = apiservice_discovery_resources(&state, &g.name, &v.version).await;
                }
                versions.push(APIVersionDiscovery {
                    version: v.version.clone(),
                    resources,
                });
            }
            items.push(APIGroupDiscovery {
                metadata: APIGroupDiscoveryMetadata {
                    name: g.name.clone(),
                },
                versions,
            });
        }

        let api_version = format!("apidiscovery.k8s.io/{}", agg_version);
        let content_type = format!(
            "application/json;g=apidiscovery.k8s.io;v={};as=APIGroupDiscoveryList",
            agg_version
        );
        let body = serde_json::to_vec(&APIGroupDiscoveryList {
            api_version,
            kind: "APIGroupDiscoveryList".to_string(),
            metadata: serde_json::json!({}),
            items,
        })
        .unwrap_or_default();

        return axum::http::Response::builder()
            .status(200)
            .header("content-type", content_type)
            .body(Body::from(body))
            .unwrap()
            .into_response();
    }

    Json(APIGroupList {
        kind: "APIGroupList".to_string(),
        api_version: "v1".to_string(),
        groups,
    })
    .into_response()
}

/// Handler for GET /apis/{group} — returns APIGroup for a specific API group.
/// K8s clients call this to discover available versions for a group.
pub async fn api_group_by_name(
    State(state): State<Arc<AppState>>,
    Path(group): Path<String>,
) -> Result<Json<Value>, crate::api::AppError> {
    // Static groups
    let static_groups: &[(&str, &str)] = &[
        ("apps", "v1"),
        ("batch", "v1"),
        ("coordination.k8s.io", "v1"),
        ("discovery.k8s.io", "v1"),
        ("events.k8s.io", "v1"),
        ("networking.k8s.io", "v1"),
        ("storage.k8s.io", "v1"),
        ("node.k8s.io", "v1"),
        ("policy", "v1"),
        ("rbac.authorization.k8s.io", "v1"),
        ("authorization.k8s.io", "v1"),
        ("certificates.k8s.io", "v1"),
        ("apiextensions.k8s.io", "v1"),
        ("admissionregistration.k8s.io", "v1"),
        ("scheduling.k8s.io", "v1"),
        ("flowcontrol.apiserver.k8s.io", "v1"),
        ("apiregistration.k8s.io", "v1"),
        ("authentication.k8s.io", "v1"),
    ];

    for &(name, version) in static_groups {
        if name == group {
            return Ok(Json(serde_json::json!({
                "kind": "APIGroup",
                "apiVersion": "v1",
                "name": name,
                "versions": [{"groupVersion": format!("{}/{}", name, version), "version": version}],
                "preferredVersion": {"groupVersion": format!("{}/{}", name, version), "version": version}
            })));
        }
    }

    // autoscaling has multiple versions (v2 preferred, v1 also available)
    if group == "autoscaling" {
        return Ok(Json(serde_json::json!({
            "kind": "APIGroup",
            "apiVersion": "v1",
            "name": "autoscaling",
            "versions": [
                {"groupVersion": "autoscaling/v2", "version": "v2"},
                {"groupVersion": "autoscaling/v1", "version": "v1"}
            ],
            "preferredVersion": {"groupVersion": "autoscaling/v2", "version": "v2"}
        })));
    }

    // Check CRD registry for dynamic groups
    let crd_versions_by_group = state.crd_registry.list_versions_by_group().await;
    if let Some(versions) = crd_versions_by_group.get(&group)
        && !versions.is_empty()
    {
        let preferred = versions
            .first()
            .expect("CRD group must have at least one served version");
        let versions_json: Vec<Value> = versions
            .iter()
            .map(|v| {
                serde_json::json!({
                    "groupVersion": format!("{}/{}", group, v),
                    "version": v
                })
            })
            .collect();
        return Ok(Json(serde_json::json!({
            "kind": "APIGroup",
            "apiVersion": "v1",
            "name": group,
            "versions": versions_json,
            "preferredVersion": {
                "groupVersion": format!("{}/{}", group, preferred),
                "version": preferred
            }
        })));
    }

    // Check APIService registrations for dynamic aggregated groups
    if let Ok(api_service_groups) = apiservice_group_versions(&state).await
        && let Some(versions_set) = api_service_groups.get(&group)
    {
        let versions: Vec<String> = versions_set.iter().cloned().collect();
        if !versions.is_empty() {
            let versions_json: Vec<Value> = versions
                .iter()
                .map(|v| {
                    serde_json::json!({
                        "groupVersion": format!("{}/{}", group, v),
                        "version": v
                    })
                })
                .collect();
            return Ok(Json(serde_json::json!({
                "kind": "APIGroup",
                "apiVersion": "v1",
                "name": group,
                "versions": versions_json,
                "preferredVersion": {
                    "groupVersion": format!("{}/{}", group, versions[0]),
                    "version": versions[0]
                }
            })));
        }
    }

    Err(crate::api::AppError::NotFound(
        "the server could not find the requested resource".to_string(),
    ))
}

/// Returns the list of resource descriptors for aggregated discovery for a known group/version.
/// For CRD groups or unknown groups, returns an empty vec (group appears but with no resources listed).
pub fn aggregated_resources_for_group_version(
    group: &str,
    _version: &str,
) -> Vec<APIResourceDiscovery> {
    let std_verbs = || {
        vec![
            "create".to_string(),
            "delete".to_string(),
            "deletecollection".to_string(),
            "get".to_string(),
            "list".to_string(),
            "patch".to_string(),
            "update".to_string(),
            "watch".to_string(),
        ]
    };

    match group {
        "apps" => vec![
            APIResourceDiscovery {
                resource: "deployments".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "Deployment".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "deployment".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: Some(vec!["deploy".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResourceDiscovery {
                resource: "replicasets".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ReplicaSet".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "replicaset".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: Some(vec!["rs".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResourceDiscovery {
                resource: "statefulsets".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "StatefulSet".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "statefulset".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: Some(vec!["sts".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResourceDiscovery {
                resource: "daemonsets".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "DaemonSet".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "daemonset".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: Some(vec!["ds".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
            APIResourceDiscovery {
                resource: "controllerrevisions".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ControllerRevision".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "controllerrevision".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: None,
                categories: None,
            },
        ],
        "batch" => vec![
            APIResourceDiscovery {
                resource: "jobs".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "Job".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "job".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: None,
                categories: Some(vec!["all".to_string()]),
            },
            APIResourceDiscovery {
                resource: "cronjobs".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "CronJob".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "cronjob".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                short_names: Some(vec!["cj".to_string()]),
                categories: Some(vec!["all".to_string()]),
            },
        ],
        "coordination.k8s.io" => vec![APIResourceDiscovery {
            resource: "leases".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "Lease".to_string(),
            },
            scope: "Namespaced".to_string(),
            singular_resource: "lease".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "discovery.k8s.io" => vec![APIResourceDiscovery {
            resource: "endpointslices".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "EndpointSlice".to_string(),
            },
            scope: "Namespaced".to_string(),
            singular_resource: "endpointslice".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "events.k8s.io" => vec![APIResourceDiscovery {
            resource: "events".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "Event".to_string(),
            },
            scope: "Namespaced".to_string(),
            singular_resource: "event".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "networking.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "ingresses".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "Ingress".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "ingress".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "ingressclasses".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "IngressClass".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "ingressclass".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            // P0-E2E-20260423-10: ServiceCIDR + IPAddress (GA in v1.31).
            // Conformance test exercises CRUD/list/watch via discovery; the
            // generic resource handlers below already support every kind, so
            // surfacing them in discovery is sufficient for the conformance
            // contract.
            APIResourceDiscovery {
                resource: "servicecidrs".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ServiceCIDR".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "servicecidr".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "ipaddresses".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "IPAddress".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "ipaddress".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        "storage.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "storageclasses".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "StorageClass".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "storageclass".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "csinodes".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "CSINode".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "csinode".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "volumeattachments".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "VolumeAttachment".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "volumeattachment".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "csistoragecapacities".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "CSIStorageCapacity".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "csistoragecapacity".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            // P0-E2E-20260423-15 part 1: CSIDriver discovery. Protobuf
            // codec already round-trips CSIDriver (src/protobuf.rs); the
            // generic resource handlers cover CRUD. Discovery registration
            // is what conformance and `kubectl get csidrivers` need.
            APIResourceDiscovery {
                resource: "csidrivers".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "CSIDriver".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "csidriver".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        "policy" => vec![APIResourceDiscovery {
            resource: "poddisruptionbudgets".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "PodDisruptionBudget".to_string(),
            },
            scope: "Namespaced".to_string(),
            singular_resource: "poddisruptionbudget".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "rbac.authorization.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "clusterroles".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ClusterRole".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "clusterrole".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "clusterrolebindings".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ClusterRoleBinding".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "clusterrolebinding".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "roles".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "Role".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "role".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "rolebindings".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "RoleBinding".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: "rolebinding".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        "certificates.k8s.io" => vec![APIResourceDiscovery {
            resource: "certificatesigningrequests".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "CertificateSigningRequest".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "certificatesigningrequest".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "apiextensions.k8s.io" => vec![APIResourceDiscovery {
            resource: "customresourcedefinitions".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "CustomResourceDefinition".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "customresourcedefinition".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "admissionregistration.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "mutatingwebhookconfigurations".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "MutatingWebhookConfiguration".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "mutatingwebhookconfiguration".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "validatingwebhookconfigurations".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ValidatingWebhookConfiguration".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "validatingwebhookconfiguration".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "validatingadmissionpolicies".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ValidatingAdmissionPolicy".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "validatingadmissionpolicy".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "validatingadmissionpolicybindings".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "ValidatingAdmissionPolicyBinding".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "validatingadmissionpolicybinding".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        "scheduling.k8s.io" => vec![APIResourceDiscovery {
            resource: "priorityclasses".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "PriorityClass".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "priorityclass".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "flowcontrol.apiserver.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "flowschemas".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "FlowSchema".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "flowschema".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "prioritylevelconfigurations".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "PriorityLevelConfiguration".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "prioritylevelconfiguration".to_string(),
                verbs: std_verbs(),
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "flowschemas/status".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "FlowSchema".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "prioritylevelconfigurations/status".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "PriorityLevelConfiguration".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: "".to_string(),
                verbs: vec!["get".to_string(), "patch".to_string(), "update".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        "autoscaling" => vec![APIResourceDiscovery {
            resource: "horizontalpodautoscalers".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "HorizontalPodAutoscaler".to_string(),
            },
            scope: "Namespaced".to_string(),
            singular_resource: "horizontalpodautoscaler".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "node.k8s.io" => vec![APIResourceDiscovery {
            resource: "runtimeclasses".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "RuntimeClass".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "runtimeclass".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "apiregistration.k8s.io" => vec![APIResourceDiscovery {
            resource: "apiservices".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "APIService".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "apiservice".to_string(),
            verbs: std_verbs(),
            subresources: Vec::new(),
            ..Default::default()
        }],
        "authentication.k8s.io" => vec![APIResourceDiscovery {
            resource: "tokenreviews".to_string(),
            response_kind: APIResourceResponseKind {
                kind: "TokenReview".to_string(),
            },
            scope: "Cluster".to_string(),
            singular_resource: "tokenreview".to_string(),
            verbs: vec!["create".to_string()],
            subresources: Vec::new(),
            ..Default::default()
        }],
        "authorization.k8s.io" => vec![
            APIResourceDiscovery {
                resource: "selfsubjectaccessreviews".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "SelfSubjectAccessReview".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: String::new(),
                verbs: vec!["create".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "selfsubjectrulesreviews".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "SelfSubjectRulesReview".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: String::new(),
                verbs: vec!["create".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "subjectaccessreviews".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "SubjectAccessReview".to_string(),
                },
                scope: "Cluster".to_string(),
                singular_resource: String::new(),
                verbs: vec!["create".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
            APIResourceDiscovery {
                resource: "localsubjectaccessreviews".to_string(),
                response_kind: APIResourceResponseKind {
                    kind: "LocalSubjectAccessReview".to_string(),
                },
                scope: "Namespaced".to_string(),
                singular_resource: String::new(),
                verbs: vec!["create".to_string()],
                subresources: Vec::new(),
                ..Default::default()
            },
        ],
        // CRD groups or unknown groups: no resources in aggregated discovery
        _ => vec![],
    }
}
