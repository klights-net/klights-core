use super::*;
pub async fn api_versions(headers: HeaderMap) -> Response {
    if let Some(agg_version) = wants_aggregated_discovery(&headers) {
        let content_type = format!(
            "application/json;g=apidiscovery.k8s.io;v={};as=APIGroupDiscoveryList",
            agg_version
        );
        let body = serde_json::to_vec(&APIGroupDiscoveryList {
            api_version: format!("apidiscovery.k8s.io/{}", agg_version),
            kind: "APIGroupDiscoveryList".to_string(),
            metadata: serde_json::json!({}),
            items: vec![APIGroupDiscovery {
                metadata: APIGroupDiscoveryMetadata {
                    name: "".to_string(),
                },
                versions: vec![APIVersionDiscovery {
                    version: "v1".to_string(),
                    resources: core_v1_aggregated_resources(),
                }],
            }],
        })
        .unwrap_or_default();

        return axum::http::Response::builder()
            .status(200)
            .header("content-type", content_type)
            .body(Body::from(body))
            .unwrap()
            .into_response();
    }

    Json(APIVersions {
        kind: "APIVersions".to_string(),
        versions: vec!["v1".to_string()],
        server_address_by_client_cidrs: vec![ServerAddressByClientCIDR {
            client_cidr: "0.0.0.0/0".to_string(),
            server_address: "".to_string(),
        }],
    })
    .into_response()
}

#[derive(Serialize)]
pub struct APIResourceList {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    #[serde(serialize_with = "serialize_primary_api_resources")]
    pub resources: Vec<APIResource>,
}

#[derive(Clone)]
pub struct APIResource {
    pub name: String,
    pub singular_name: String,
    pub namespaced: bool,
    pub kind: String,
    pub verbs: Vec<String>,
    pub short_names: Option<Vec<String>>,
    pub categories: Option<Vec<String>>,
}

impl Serialize for APIResource {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Primary resources (no "/" in the name) advertise a storageVersionHash;
        // subresources (e.g. "pods/status") do not, matching upstream.
        let is_primary = !self.name.contains('/');
        let mut field_count = 5; // name, singularName, namespaced, kind, verbs
        if self.short_names.is_some() {
            field_count += 1;
        }
        if self.categories.is_some() {
            field_count += 1;
        }
        if is_primary {
            field_count += 1;
        }
        let mut state = serializer.serialize_struct("APIResource", field_count)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("singularName", &self.singular_name)?;
        state.serialize_field("namespaced", &self.namespaced)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("verbs", &self.verbs)?;
        if let Some(ref short_names) = self.short_names {
            state.serialize_field("shortNames", short_names)?;
        }
        // Omit categories when absent (upstream omits rather than emitting null).
        if let Some(ref categories) = self.categories {
            state.serialize_field("categories", categories)?;
        }
        if is_primary {
            state.serialize_field(
                "storageVersionHash",
                &super::shared::storage_version_hash_for(&self.kind),
            )?;
        }
        state.end()
    }
}

fn core_v1_api_resources() -> Vec<APIResource> {
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

    vec![
        APIResource {
            name: "pods".to_string(),
            singular_name: "pod".to_string(),
            namespaced: true,
            kind: "Pod".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["po".to_string()]),
            categories: Some(vec!["all".to_string()]),
        },
        APIResource {
            name: "services".to_string(),
            singular_name: "service".to_string(),
            namespaced: true,
            kind: "Service".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["svc".to_string()]),
            categories: Some(vec!["all".to_string()]),
        },
        APIResource {
            name: "endpoints".to_string(),
            singular_name: "endpoints".to_string(),
            namespaced: true,
            kind: "Endpoints".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["ep".to_string()]),
            categories: None,
        },
        APIResource {
            name: "configmaps".to_string(),
            singular_name: "configmap".to_string(),
            namespaced: true,
            kind: "ConfigMap".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["cm".to_string()]),
            categories: None,
        },
        APIResource {
            name: "secrets".to_string(),
            singular_name: "secret".to_string(),
            namespaced: true,
            kind: "Secret".to_string(),
            verbs: standard_verbs.clone(),
            short_names: None,
            categories: None,
        },
        APIResource {
            name: "persistentvolumeclaims".to_string(),
            singular_name: "persistentvolumeclaim".to_string(),
            namespaced: true,
            kind: "PersistentVolumeClaim".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["pvc".to_string()]),
            categories: None,
        },
        APIResource {
            name: "serviceaccounts".to_string(),
            singular_name: "serviceaccount".to_string(),
            namespaced: true,
            kind: "ServiceAccount".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["sa".to_string()]),
            categories: None,
        },
        APIResource {
            name: "events".to_string(),
            singular_name: "event".to_string(),
            namespaced: true,
            kind: "Event".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["ev".to_string()]),
            categories: None,
        },
        APIResource {
            name: "replicationcontrollers".to_string(),
            singular_name: "replicationcontroller".to_string(),
            namespaced: true,
            kind: "ReplicationController".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["rc".to_string()]),
            categories: Some(vec!["all".to_string()]),
        },
        APIResource {
            name: "limitranges".to_string(),
            singular_name: "limitrange".to_string(),
            namespaced: true,
            kind: "LimitRange".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["limits".to_string()]),
            categories: None,
        },
        APIResource {
            name: "resourcequotas".to_string(),
            singular_name: "resourcequota".to_string(),
            namespaced: true,
            kind: "ResourceQuota".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["quota".to_string()]),
            categories: None,
        },
        APIResource {
            name: "podtemplates".to_string(),
            singular_name: "podtemplate".to_string(),
            namespaced: true,
            kind: "PodTemplate".to_string(),
            verbs: standard_verbs.clone(),
            short_names: None,
            categories: None,
        },
        APIResource {
            name: "namespaces".to_string(),
            singular_name: "namespace".to_string(),
            namespaced: false,
            kind: "Namespace".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["ns".to_string()]),
            categories: None,
        },
        APIResource {
            name: "nodes".to_string(),
            singular_name: "node".to_string(),
            namespaced: false,
            kind: "Node".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["no".to_string()]),
            categories: None,
        },
        APIResource {
            name: "persistentvolumes".to_string(),
            singular_name: "persistentvolume".to_string(),
            namespaced: false,
            kind: "PersistentVolume".to_string(),
            verbs: standard_verbs.clone(),
            short_names: Some(vec!["pv".to_string()]),
            categories: None,
        },
        // Subresources — mirror upstream /api/v1. `kind` is the represented
        // object's kind (Scale/Binding/Eviction/TokenRequest/etc. for action
        // subresources, the parent kind for status).
        subresource("pods/attach", true, "PodAttachOptions", &["create", "get"]),
        subresource("pods/binding", true, "Binding", &["create"]),
        subresource("pods/eviction", true, "Eviction", &["create"]),
        subresource("pods/exec", true, "PodExecOptions", &["create", "get"]),
        subresource("pods/log", true, "Pod", &["get"]),
        subresource(
            "pods/portforward",
            true,
            "PodPortForwardOptions",
            &["create", "get"],
        ),
        subresource(
            "pods/proxy",
            true,
            "PodProxyOptions",
            &["create", "delete", "get", "patch", "update"],
        ),
        subresource("pods/status", true, "Pod", &["get", "patch", "update"]),
        subresource(
            "pods/ephemeralcontainers",
            true,
            "Pod",
            &["get", "patch", "update"],
        ),
        subresource(
            "services/proxy",
            true,
            "ServiceProxyOptions",
            &["create", "delete", "get", "patch", "update"],
        ),
        subresource(
            "services/status",
            true,
            "Service",
            &["get", "patch", "update"],
        ),
        subresource(
            "replicationcontrollers/scale",
            true,
            "Scale",
            &["get", "patch", "update"],
        ),
        subresource(
            "replicationcontrollers/status",
            true,
            "ReplicationController",
            &["get", "patch", "update"],
        ),
        subresource(
            "persistentvolumeclaims/status",
            true,
            "PersistentVolumeClaim",
            &["get", "patch", "update"],
        ),
        subresource(
            "resourcequotas/status",
            true,
            "ResourceQuota",
            &["get", "patch", "update"],
        ),
        subresource("serviceaccounts/token", true, "TokenRequest", &["create"]),
        subresource(
            "namespaces/status",
            false,
            "Namespace",
            &["get", "patch", "update"],
        ),
        subresource("namespaces/finalize", false, "Namespace", &["update"]),
        subresource(
            "nodes/proxy",
            false,
            "NodeProxyOptions",
            &["create", "delete", "get", "patch", "update"],
        ),
        subresource("nodes/status", false, "Node", &["get", "patch", "update"]),
        subresource(
            "persistentvolumes/status",
            false,
            "PersistentVolume",
            &["get", "patch", "update"],
        ),
    ]
}

/// Build a subresource `APIResource` (no singular name, short names, categories
/// or storageVersionHash — matching upstream subresource discovery entries).
fn subresource(name: &str, namespaced: bool, kind: &str, verbs: &[&str]) -> APIResource {
    APIResource {
        name: name.to_string(),
        singular_name: String::new(),
        namespaced,
        kind: kind.to_string(),
        verbs: verbs.iter().map(|v| v.to_string()).collect(),
        short_names: None,
        categories: None,
    }
}

fn serialize_primary_api_resources<S>(
    resources: &[APIResource],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // Keep subresources (for example */status) in discovery output.
    // Kubernetes conformance expects these to be listed.
    let mut seq = serializer.serialize_seq(Some(resources.len()))?;
    for resource in resources {
        seq.serialize_element(resource)?;
    }
    seq.end()
}

pub async fn api_v1_resources() -> Json<APIResourceList> {
    Json(APIResourceList {
        kind: "APIResourceList".to_string(),
        api_version: "v1".to_string(),
        group_version: "v1".to_string(),
        resources: core_v1_api_resources(),
    })
}

#[derive(Serialize)]
pub struct APIGroupList {
    pub kind: String,
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub groups: Vec<APIGroup>,
}

#[derive(Serialize)]
pub struct APIGroup {
    pub name: String,
    pub versions: Vec<GroupVersionForDiscovery>,
    #[serde(rename = "preferredVersion")]
    pub preferred_version: GroupVersionForDiscovery,
}

#[derive(Serialize, Clone)]
pub struct GroupVersionForDiscovery {
    #[serde(rename = "groupVersion")]
    pub group_version: String,
    pub version: String,
}

/// Aggregated discovery types (apidiscovery.k8s.io/v2)
#[derive(Serialize)]
pub struct APIGroupDiscoveryList {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: serde_json::Value,
    pub items: Vec<APIGroupDiscovery>,
}

#[derive(Serialize)]
pub struct APIGroupDiscovery {
    pub metadata: APIGroupDiscoveryMetadata,
    pub versions: Vec<APIVersionDiscovery>,
}

#[derive(Serialize)]
pub struct APIGroupDiscoveryMetadata {
    pub name: String,
}

#[derive(Serialize)]
pub struct APIVersionDiscovery {
    pub version: String,
    pub resources: Vec<APIResourceDiscovery>,
}

#[derive(Serialize, Default, Clone)]
pub struct APIResourceDiscovery {
    pub resource: String,
    #[serde(rename = "responseKind")]
    pub response_kind: APIResourceResponseKind,
    pub scope: String,
    #[serde(rename = "singularResource")]
    pub singular_resource: String,
    pub verbs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "shortNames")]
    pub short_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub categories: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub subresources: Vec<APISubresourceDiscovery>,
}

#[derive(Serialize, Default, Clone)]
pub struct APISubresourceDiscovery {
    pub subresource: String,
    #[serde(rename = "responseKind")]
    pub response_kind: APIResourceResponseKind,
    pub verbs: Vec<String>,
}

#[derive(Serialize, Default, Clone)]
pub struct APIResourceResponseKind {
    pub kind: String,
}

/// Convert a flat `APIResource` list (primary resources + their `parent/sub`
/// subresource entries) into aggregated discovery (`apidiscovery.k8s.io/v2`)
/// form, where subresources are nested under their parent's `subresources[]`
/// rather than listed as siblings.
pub fn nest_aggregated_resources(flat: Vec<APIResource>) -> Vec<APIResourceDiscovery> {
    let mut primaries: Vec<APIResourceDiscovery> = Vec::new();
    let mut sub_index: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // First pass: primary resources establish the parents.
    for resource in &flat {
        if resource.name.contains('/') {
            continue;
        }
        sub_index.insert(resource.name.clone(), primaries.len());
        primaries.push(APIResourceDiscovery {
            resource: resource.name.clone(),
            response_kind: APIResourceResponseKind {
                kind: resource.kind.clone(),
            },
            scope: if resource.namespaced {
                "Namespaced".to_string()
            } else {
                "Cluster".to_string()
            },
            singular_resource: resource.singular_name.clone(),
            verbs: resource.verbs.clone(),
            short_names: resource.short_names.clone(),
            categories: resource.categories.clone(),
            subresources: Vec::new(),
        });
    }

    // Second pass: nest subresources under their parent.
    for resource in flat {
        let Some((parent, sub)) = resource.name.split_once('/') else {
            continue;
        };
        if let Some(&idx) = sub_index.get(parent) {
            primaries[idx].subresources.push(APISubresourceDiscovery {
                subresource: sub.to_string(),
                response_kind: APIResourceResponseKind {
                    kind: resource.kind,
                },
                verbs: resource.verbs,
            });
        }
    }

    primaries
}

pub fn core_v1_aggregated_resources() -> Vec<APIResourceDiscovery> {
    nest_aggregated_resources(core_v1_api_resources())
}
