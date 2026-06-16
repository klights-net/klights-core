//! Kubernetes default RBAC fixtures and bootstrap seeding helper.
//!
//! Phase 3.2 moves default RBAC object definitions into explicit typed
//! fixtures so bootstrap and reconcile paths share one source of truth.

use serde_json::json;

pub const RBAC_API_VERSION: &str = "rbac.authorization.k8s.io/v1";
pub const AUTOUPDATE_ANNOTATION: &str = "rbac.authorization.kubernetes.io/autoupdate";
const BOOTSTRAP_LABEL: (&str, &str) = ("kubernetes.io/bootstrapping", "rbac-defaults");

#[derive(Clone, Debug)]
pub struct DefaultRbacRule {
    pub verbs: Vec<&'static str>,
    pub api_groups: Vec<&'static str>,
    pub resources: Vec<&'static str>,
    pub resource_names: Vec<&'static str>,
    pub non_resource_urls: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub struct DefaultRbacRoleRef {
    pub api_group: &'static str,
    pub kind: &'static str,
    pub name: &'static str,
}

#[derive(Clone, Debug)]
pub struct DefaultRbacSubject {
    pub kind: &'static str,
    pub api_group: Option<&'static str>,
    pub name: &'static str,
    pub namespace: Option<&'static str>,
}

#[derive(Clone, Debug)]
pub struct DefaultRbacObject {
    pub kind: &'static str,
    pub name: &'static str,
    pub namespace: Option<&'static str>,
    pub labels: Vec<(&'static str, &'static str)>,
    pub annotations: Vec<(&'static str, &'static str)>,
    pub rules: Option<Vec<DefaultRbacRule>>,
    pub role_ref: Option<DefaultRbacRoleRef>,
    pub subjects: Option<Vec<DefaultRbacSubject>>,
    /// `aggregationRule.clusterRoleSelectors` for aggregated ClusterRoles
    /// (admin/edit/view). Each inner vec is one selector's `matchLabels`. The
    /// reconciler recomputes `rules` as the union of the role's own default
    /// rules and the rules of every ClusterRole matching any of these
    /// selectors, so granted privilege is revoked when a source role loses the
    /// label or is deleted.
    pub aggregation_rule: Option<Vec<Vec<(&'static str, &'static str)>>>,
}

impl DefaultRbacObject {
    fn aggregated_to(mut self, aggregate_label: &'static str) -> Self {
        self.aggregation_rule = Some(vec![vec![(aggregate_label, "true")]]);
        self
    }

    fn metadata_labels(&self) -> Vec<(&'static str, &'static str)> {
        let mut labels = vec![BOOTSTRAP_LABEL; 1];
        labels.extend(self.labels.iter().cloned());
        labels
    }

    fn metadata_annotations(&self) -> Vec<(&'static str, &'static str)> {
        let mut annotations = vec![(AUTOUPDATE_ANNOTATION, "true")];
        annotations.extend(self.annotations.iter().cloned());
        annotations
    }

    pub fn key(&self) -> (&'static str, &'static str, Option<&'static str>) {
        (self.kind, self.name, self.namespace)
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        let mut labels = serde_json::Map::new();
        for (key, value) in self.metadata_labels() {
            labels.insert(key.to_string(), json!(value));
        }

        let mut annotations = serde_json::Map::new();
        for (key, value) in self.metadata_annotations() {
            annotations.insert(key.to_string(), json!(value));
        }

        let mut metadata = serde_json::Map::new();
        metadata.insert("name".to_string(), json!(self.name));
        if let Some(namespace) = self.namespace {
            metadata.insert("namespace".to_string(), json!(namespace));
        }
        metadata.insert("labels".to_string(), serde_json::Value::Object(labels));
        metadata.insert(
            "annotations".to_string(),
            serde_json::Value::Object(annotations),
        );

        let mut object = serde_json::Map::new();
        object.insert("apiVersion".to_string(), json!(RBAC_API_VERSION));
        object.insert("kind".to_string(), json!(self.kind));
        object.insert("metadata".to_string(), serde_json::Value::Object(metadata));

        match self.kind {
            "ClusterRole" | "Role" => {
                let rules: Vec<serde_json::Value> = self
                    .rules
                    .as_ref()
                    .map(|rules| {
                        rules
                            .iter()
                            .map(|rule| {
                                json!({
                                    "verbs": rule.verbs,
                                    "apiGroups": rule.api_groups,
                                    "resources": rule.resources,
                                    "resourceNames": rule.resource_names,
                                    "nonResourceURLs": rule.non_resource_urls,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                object.insert("rules".to_string(), json!(rules));

                if let Some(selectors) = self.aggregation_rule.as_ref() {
                    let cluster_role_selectors: Vec<serde_json::Value> = selectors
                        .iter()
                        .map(|match_labels| {
                            let mut labels = serde_json::Map::new();
                            for (key, value) in match_labels {
                                labels.insert(key.to_string(), json!(value));
                            }
                            json!({"matchLabels": serde_json::Value::Object(labels)})
                        })
                        .collect();
                    object.insert(
                        "aggregationRule".to_string(),
                        json!({"clusterRoleSelectors": cluster_role_selectors}),
                    );
                }
            }
            "ClusterRoleBinding" => {
                let role_ref = self.role_ref.as_ref().expect("clusterrolebinding roleRef");
                let subjects: Vec<serde_json::Value> = self
                    .subjects
                    .as_ref()
                    .expect("clusterrolebinding subjects")
                    .iter()
                    .map(|s| {
                        let mut value = serde_json::Map::new();
                        value.insert("kind".to_string(), json!(s.kind));
                        if let Some(api_group) = s.api_group {
                            value.insert("apiGroup".to_string(), json!(api_group));
                        }
                        value.insert("name".to_string(), json!(s.name));
                        if let Some(namespace) = s.namespace {
                            value.insert("namespace".to_string(), json!(namespace));
                        }
                        serde_json::Value::Object(value)
                    })
                    .collect();

                object.insert(
                    "roleRef".to_string(),
                    json!({
                        "apiGroup": role_ref.api_group,
                        "kind": role_ref.kind,
                        "name": role_ref.name
                    }),
                );
                object.insert("subjects".to_string(), json!(subjects));
            }
            _ => {}
        }

        serde_json::Value::Object(object)
    }
}

fn cluster_role(
    name: &'static str,
    labels: Vec<(&'static str, &'static str)>,
    annotations: Vec<(&'static str, &'static str)>,
    rules: Vec<DefaultRbacRule>,
) -> DefaultRbacObject {
    DefaultRbacObject {
        kind: "ClusterRole",
        name,
        namespace: None,
        labels,
        annotations,
        rules: Some(rules),
        role_ref: None,
        subjects: None,
        aggregation_rule: None,
    }
}

fn role(
    namespace: &'static str,
    name: &'static str,
    labels: Vec<(&'static str, &'static str)>,
    annotations: Vec<(&'static str, &'static str)>,
    rules: Vec<DefaultRbacRule>,
) -> DefaultRbacObject {
    DefaultRbacObject {
        kind: "Role",
        name,
        namespace: Some(namespace),
        labels,
        annotations,
        rules: Some(rules),
        role_ref: None,
        subjects: None,
        aggregation_rule: None,
    }
}

fn cluster_role_binding(
    name: &'static str,
    labels: Vec<(&'static str, &'static str)>,
    annotations: Vec<(&'static str, &'static str)>,
    role_ref: DefaultRbacRoleRef,
    subjects: Vec<DefaultRbacSubject>,
) -> DefaultRbacObject {
    DefaultRbacObject {
        kind: "ClusterRoleBinding",
        name,
        namespace: None,
        labels,
        annotations,
        rules: None,
        role_ref: Some(role_ref),
        subjects: Some(subjects),
        aggregation_rule: None,
    }
}

fn bootstrap_defaults() -> Vec<DefaultRbacObject> {
    vec![
        role(
            "kube-system",
            "extension-apiserver-authentication-reader",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["get", "list", "watch"],
                api_groups: vec![""],
                resources: vec!["configmaps"],
                resource_names: vec!["extension-apiserver-authentication"],
                non_resource_urls: vec![],
            }],
        ),
        cluster_role(
            "system:discovery",
            vec![("rbac.authorization.k8s.io/aggregate-to-edit", "true")],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["get"],
                api_groups: vec![""],
                resources: vec![],
                resource_names: vec![],
                non_resource_urls: vec![
                    "/api",
                    "/api/*",
                    "/apis",
                    "/apis/*",
                    "/healthz",
                    "/livez",
                    "/readyz",
                    "/openapi",
                    "/openapi/*",
                    "/version",
                    "/version/",
                ],
            }],
        ),
        cluster_role_binding(
            "system:authenticated:discovery",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:discovery",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:authenticated",
                namespace: None,
            }],
        ),
        // system:basic-user lets any authenticated user introspect their own
        // access (SelfSubjectAccessReview / SelfSubjectRulesReview). Required so
        // the global authorization chokepoint does not deny these for users who
        // hold no other RBAC grants — matches upstream Kubernetes.
        cluster_role(
            "system:basic-user",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["create"],
                api_groups: vec!["authorization.k8s.io"],
                resources: vec![
                    "selfsubjectaccessreviews",
                    "selfsubjectrulesreviews",
                    "selfsubjectreviews",
                ],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
        ),
        cluster_role_binding(
            "system:basic-user",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:basic-user",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:authenticated",
                namespace: None,
            }],
        ),
        // system:public-info-viewer grants unauthenticated (and authenticated)
        // access to the public, non-sensitive informational endpoints, matching
        // upstream Kubernetes. These requests still flow through the global
        // authorizer as non-resource URL checks.
        cluster_role(
            "system:public-info-viewer",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["get"],
                api_groups: vec![""],
                resources: vec![],
                resource_names: vec![],
                non_resource_urls: vec!["/healthz", "/livez", "/readyz", "/version", "/version/"],
            }],
        ),
        cluster_role_binding(
            "system:public-info-viewer",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:public-info-viewer",
            },
            vec![
                DefaultRbacSubject {
                    kind: "Group",
                    api_group: Some("rbac.authorization.k8s.io"),
                    name: "system:authenticated",
                    namespace: None,
                },
                DefaultRbacSubject {
                    kind: "Group",
                    api_group: Some("rbac.authorization.k8s.io"),
                    name: "system:unauthenticated",
                    namespace: None,
                },
            ],
        ),
        cluster_role(
            "system:monitoring",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["get"],
                api_groups: vec![""],
                resources: vec![],
                resource_names: vec![],
                non_resource_urls: vec![
                    "/healthz",
                    "/healthz/*",
                    "/livez",
                    "/livez/*",
                    "/readyz",
                    "/readyz/*",
                    "/metrics",
                    "/metrics/slis",
                ],
            }],
        ),
        cluster_role_binding(
            "system:monitoring",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:monitoring",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:monitoring",
                namespace: None,
            }],
        ),
        cluster_role(
            "system:service-account-issuer-discovery",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["get"],
                api_groups: vec![""],
                resources: vec![],
                resource_names: vec![],
                non_resource_urls: vec![
                    "/.well-known/openid-configuration",
                    "/.well-known/openid-configuration/",
                    "/openid/v1/jwks",
                    "/openid/v1/jwks/",
                ],
            }],
        ),
        cluster_role(
            "system:node-bootstrapper",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["create", "get", "list", "watch"],
                api_groups: vec!["certificates.k8s.io"],
                resources: vec!["certificatesigningrequests"],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
        ),
        cluster_role_binding(
            "system:bootstrappers:node-bootstrapper",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:node-bootstrapper",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:bootstrappers:klights:worker",
                namespace: None,
            }],
        ),
        cluster_role(
            "system:certificates.k8s.io:certificatesigningrequests:nodeclient",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["create"],
                api_groups: vec!["certificates.k8s.io"],
                resources: vec!["certificatesigningrequests/nodeclient"],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
        ),
        cluster_role_binding(
            "system:bootstrappers:nodeclient",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:certificates.k8s.io:certificatesigningrequests:nodeclient",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:bootstrappers:klights:worker",
                namespace: None,
            }],
        ),
        cluster_role(
            "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient",
            vec![],
            vec![],
            vec![DefaultRbacRule {
                verbs: vec!["create"],
                api_groups: vec!["certificates.k8s.io"],
                resources: vec!["certificatesigningrequests/selfnodeclient"],
                resource_names: vec![],
                non_resource_urls: vec![],
            }],
        ),
        cluster_role_binding(
            "system:nodes:selfnodeclient",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:nodes",
                namespace: None,
            }],
        ),
        // system:auth-delegator enables delegated authentication and
        // authorization for aggregated API servers. Backends receive proxied
        // user identity via x-remote-* headers and verify it through
        // TokenReview and SubjectAccessReview against this ClusterRole.
        cluster_role(
            "system:auth-delegator",
            vec![],
            vec![],
            vec![
                DefaultRbacRule {
                    verbs: vec!["create"],
                    api_groups: vec!["authentication.k8s.io"],
                    resources: vec!["tokenreviews"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["create"],
                    api_groups: vec!["authorization.k8s.io"],
                    resources: vec!["subjectaccessreviews"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
            ],
        ),
        // NOTE: upstream Kubernetes does NOT bind system:auth-delegator to any
        // subject by default — cluster admins bind it explicitly to an
        // aggregated/extension API server's ServiceAccount. Binding it to
        // system:authenticated would let every authenticated principal create
        // TokenReviews/SubjectAccessReviews, so no default binding is created.
        cluster_role(
            "cluster-admin",
            vec![],
            vec![],
            // Upstream cluster-admin is TWO rules: one for every resource and
            // one for every non-resource URL. A single rule that sets both
            // `resources` and `nonResourceURLs` is malformed under Kubernetes
            // RBAC (a rule targets resources XOR nonResourceURLs) and is
            // rejected by `rule_matches`, leaving cluster-admin granting
            // nothing — a ServiceAccount bound to it gets a blanket 403.
            vec![
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["*"],
                    resources: vec!["*"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec![],
                    resources: vec![],
                    resource_names: vec![],
                    non_resource_urls: vec!["*"],
                },
            ],
        ),
        cluster_role_binding(
            "cluster-admin",
            vec![],
            vec![],
            DefaultRbacRoleRef {
                api_group: "rbac.authorization.k8s.io",
                kind: "ClusterRole",
                name: "cluster-admin",
            },
            vec![DefaultRbacSubject {
                kind: "Group",
                api_group: Some("rbac.authorization.k8s.io"),
                name: "system:masters",
                namespace: None,
            }],
        ),
        cluster_role(
            "admin",
            vec![
                ("rbac.authorization.k8s.io/aggregate-to-admin", "true"),
                ("rbac.authorization.k8s.io/aggregate-to-edit", "true"),
            ],
            vec![],
            vec![
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec![""],
                    resources: vec![
                        "pods",
                        "pods/attach",
                        "pods/exec",
                        "pods/portforward",
                        "pods/proxy",
                        "configmaps",
                        "endpoints",
                        "persistentvolumeclaims",
                        "replicationcontrollers",
                        "replicationcontrollers/scale",
                        "secrets",
                        "serviceaccounts",
                        "services",
                        "services/proxy",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["apps"],
                    resources: vec![
                        "daemonsets",
                        "deployments",
                        "deployments/scale",
                        "replicasets",
                        "replicasets/scale",
                        "statefulsets",
                        "statefulsets/scale",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["batch"],
                    resources: vec!["cronjobs", "jobs"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["autoscaling"],
                    resources: vec!["horizontalpodautoscalers"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["networking.k8s.io"],
                    resources: vec!["ingresses", "networkpolicies"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["policy"],
                    resources: vec!["poddisruptionbudgets"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["rbac.authorization.k8s.io"],
                    resources: vec!["roles", "rolebindings"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec![""],
                    resources: vec!["namespaces"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec![""],
                    resources: vec!["resourcequotas", "limitranges"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
            ],
        )
        .aggregated_to("rbac.authorization.k8s.io/aggregate-to-admin"),
        cluster_role(
            "edit",
            vec![("rbac.authorization.k8s.io/aggregate-to-edit", "true")],
            vec![],
            vec![
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec![""],
                    resources: vec![
                        "pods",
                        "pods/attach",
                        "pods/exec",
                        "pods/portforward",
                        "pods/proxy",
                        "configmaps",
                        "endpoints",
                        "persistentvolumeclaims",
                        "replicationcontrollers",
                        "replicationcontrollers/scale",
                        "secrets",
                        "serviceaccounts",
                        "services",
                        "services/proxy",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["apps"],
                    resources: vec![
                        "daemonsets",
                        "deployments",
                        "deployments/scale",
                        "replicasets",
                        "replicasets/scale",
                        "statefulsets",
                        "statefulsets/scale",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["batch"],
                    resources: vec!["cronjobs", "jobs"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["autoscaling"],
                    resources: vec!["horizontalpodautoscalers"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["networking.k8s.io"],
                    resources: vec!["ingresses", "networkpolicies"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["*"],
                    api_groups: vec!["policy"],
                    resources: vec!["poddisruptionbudgets"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
            ],
        )
        .aggregated_to("rbac.authorization.k8s.io/aggregate-to-edit"),
        cluster_role(
            "view",
            vec![("rbac.authorization.k8s.io/aggregate-to-view", "true")],
            vec![],
            vec![
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec![""],
                    resources: vec![
                        "pods",
                        "configmaps",
                        "endpoints",
                        "persistentvolumeclaims",
                        "replicationcontrollers",
                        "replicationcontrollers/scale",
                        "serviceaccounts",
                        "services",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec!["apps"],
                    resources: vec![
                        "daemonsets",
                        "deployments",
                        "deployments/scale",
                        "replicasets",
                        "replicasets/scale",
                        "statefulsets",
                        "statefulsets/scale",
                    ],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec!["batch"],
                    resources: vec!["cronjobs", "jobs"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec!["autoscaling"],
                    resources: vec!["horizontalpodautoscalers"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec!["networking.k8s.io"],
                    resources: vec!["ingresses", "networkpolicies"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec!["policy"],
                    resources: vec!["poddisruptionbudgets"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
                DefaultRbacRule {
                    verbs: vec!["get", "list", "watch"],
                    api_groups: vec![""],
                    resources: vec!["namespaces"],
                    resource_names: vec![],
                    non_resource_urls: vec![],
                },
            ],
        )
        .aggregated_to("rbac.authorization.k8s.io/aggregate-to-view"),
    ]
}

pub fn default_rbac_fixtures() -> Vec<DefaultRbacObject> {
    bootstrap_defaults()
}

/// Default `rules` for a built-in ClusterRole fixture, as JSON rule objects.
/// Returns an empty vec for names that have no fixture (e.g. user-defined
/// aggregated ClusterRoles). The aggregation reconciler uses this as the
/// non-revocable floor of an aggregated role's rule set.
pub fn default_cluster_role_rules(name: &str) -> Vec<serde_json::Value> {
    default_rbac_fixtures()
        .into_iter()
        .find(|object| object.kind == "ClusterRole" && object.name == name)
        .and_then(|object| {
            object
                .to_json_value()
                .get("rules")
                .and_then(serde_json::Value::as_array)
                .cloned()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod default_roles_tests {
    use super::*;

    fn find<'a>(objs: &'a [DefaultRbacObject], kind: &str, name: &str) -> &'a DefaultRbacObject {
        objs.iter()
            .find(|o| o.kind == kind && o.name == name)
            .unwrap_or_else(|| panic!("missing {kind} {name}"))
    }

    #[test]
    fn basic_user_grants_self_review_to_authenticated() {
        let objs = default_rbac_fixtures();
        let role = find(&objs, "ClusterRole", "system:basic-user");
        let rule = &role.rules.as_ref().unwrap()[0];
        assert_eq!(rule.verbs, vec!["create"]);
        assert_eq!(rule.api_groups, vec!["authorization.k8s.io"]);
        assert!(rule.resources.contains(&"selfsubjectaccessreviews"));
        assert!(rule.resources.contains(&"selfsubjectrulesreviews"));

        let binding = find(&objs, "ClusterRoleBinding", "system:basic-user");
        assert_eq!(binding.role_ref.as_ref().unwrap().name, "system:basic-user");
        let subjects = binding.subjects.as_ref().unwrap();
        assert!(
            subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:authenticated")
        );
    }

    #[test]
    fn cluster_admin_grants_both_resource_and_non_resource_access() {
        use crate::auth::rbac_rule_evaluator::{PolicyRule, RuleMatchRequest, rule_matches};

        let objs = default_rbac_fixtures();
        let role = find(&objs, "ClusterRole", "cluster-admin");
        let rules: Vec<PolicyRule> = role
            .rules
            .as_ref()
            .unwrap()
            .iter()
            .map(|r| PolicyRule {
                verbs: r.verbs.iter().map(|s| s.to_string()).collect(),
                api_groups: r.api_groups.iter().map(|s| s.to_string()).collect(),
                resources: r.resources.iter().map(|s| s.to_string()).collect(),
                resource_names: r.resource_names.iter().map(|s| s.to_string()).collect(),
                non_resource_urls: r.non_resource_urls.iter().map(|s| s.to_string()).collect(),
            })
            .collect();

        // cluster-admin must authorize an ordinary resource request. With the
        // resource grant and the non-resource grant fused into a SINGLE rule,
        // `rule_matches` rejects it as malformed (a rule may target resources
        // XOR nonResourceURLs, never both), so cluster-admin grants nothing —
        // exactly the 403 a ServiceAccount bound to cluster-admin hit.
        let resource_req = RuleMatchRequest {
            verb: "list",
            api_group: Some(""),
            resource: Some("limitranges"),
            subresource: None,
            resource_name: None,
            non_resource_url: None,
            field_selector: None,
        };
        assert!(
            rules.iter().any(|r| rule_matches(r, resource_req)),
            "cluster-admin must grant resource access (list limitranges)"
        );

        // cluster-admin must also authorize a non-resource URL request.
        let non_resource_req = RuleMatchRequest {
            verb: "get",
            api_group: None,
            resource: None,
            subresource: None,
            resource_name: None,
            non_resource_url: Some("/healthz"),
            field_selector: None,
        };
        assert!(
            rules.iter().any(|r| rule_matches(r, non_resource_req)),
            "cluster-admin must grant non-resource URL access (/healthz)"
        );
    }

    #[test]
    fn auth_delegator_role_exists_but_has_no_default_binding() {
        let objs = default_rbac_fixtures();
        // The ClusterRole is still defined for admins to bind to extension
        // apiserver SAs explicitly.
        let _ = find(&objs, "ClusterRole", "system:auth-delegator");
        // But it must NOT be bound by default (especially not to
        // system:authenticated).
        assert!(
            !objs
                .iter()
                .any(|o| o.kind == "ClusterRoleBinding" && o.name == "system:auth-delegator"),
            "system:auth-delegator must not have a default ClusterRoleBinding"
        );
    }

    #[test]
    fn public_info_viewer_bound_to_unauthenticated() {
        let objs = default_rbac_fixtures();
        let role = find(&objs, "ClusterRole", "system:public-info-viewer");
        let rule = &role.rules.as_ref().unwrap()[0];
        assert_eq!(rule.verbs, vec!["get"]);
        assert!(rule.non_resource_urls.contains(&"/version"));
        assert!(!rule.non_resource_urls.contains(&"/metrics"));
        assert!(!rule.non_resource_urls.contains(&"/openid/v1/jwks"));
        assert!(
            rule.non_resource_urls
                .iter()
                .all(|url| !url.starts_with("/.well-known/"))
        );

        let binding = find(&objs, "ClusterRoleBinding", "system:public-info-viewer");
        let subjects = binding.subjects.as_ref().unwrap();
        assert!(
            subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:unauthenticated")
        );
        assert!(
            subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:authenticated")
        );
    }

    #[test]
    fn service_account_issuer_discovery_role_is_not_bound_by_default() {
        let objs = default_rbac_fixtures();
        let role = find(
            &objs,
            "ClusterRole",
            "system:service-account-issuer-discovery",
        );
        let rule = &role.rules.as_ref().unwrap()[0];
        assert_eq!(rule.verbs, vec!["get"]);
        assert_eq!(
            rule.non_resource_urls,
            vec![
                "/.well-known/openid-configuration",
                "/.well-known/openid-configuration/",
                "/openid/v1/jwks",
                "/openid/v1/jwks/",
            ]
        );
        assert!(objs.iter().all(|o| {
            o.kind != "ClusterRoleBinding" || o.name != "system:service-account-issuer-discovery"
        }));
    }

    #[test]
    fn discovery_bound_to_authenticated_only() {
        let objs = default_rbac_fixtures();
        let role = find(&objs, "ClusterRole", "system:discovery");
        let rule = &role.rules.as_ref().unwrap()[0];
        assert_eq!(rule.verbs, vec!["get"]);
        assert!(rule.non_resource_urls.contains(&"/api"));
        assert!(rule.non_resource_urls.contains(&"/apis/*"));
        assert!(rule.non_resource_urls.contains(&"/openapi/*"));
        assert!(!rule.non_resource_urls.contains(&"/metrics"));

        let binding = find(
            &objs,
            "ClusterRoleBinding",
            "system:authenticated:discovery",
        );
        assert_eq!(binding.role_ref.as_ref().unwrap().name, "system:discovery");
        let subjects = binding.subjects.as_ref().unwrap();
        assert!(
            subjects
                .iter()
                .any(|s| s.kind == "Group" && s.name == "system:authenticated")
        );
        assert!(
            subjects
                .iter()
                .all(|s| !(s.kind == "Group" && s.name == "system:unauthenticated"))
        );
    }

    #[test]
    fn monitoring_group_can_read_control_plane_monitoring_endpoints() {
        let objs = default_rbac_fixtures();
        let role = find(&objs, "ClusterRole", "system:monitoring");
        let rule = &role.rules.as_ref().unwrap()[0];
        assert_eq!(rule.verbs, vec!["get"]);
        for url in [
            "/healthz",
            "/healthz/*",
            "/livez",
            "/livez/*",
            "/readyz",
            "/readyz/*",
            "/metrics",
            "/metrics/slis",
        ] {
            assert!(rule.non_resource_urls.contains(&url), "{url}");
        }

        let binding = find(&objs, "ClusterRoleBinding", "system:monitoring");
        assert_eq!(binding.role_ref.as_ref().unwrap().name, "system:monitoring");
        let subjects = binding.subjects.as_ref().unwrap();
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].kind, "Group");
        assert_eq!(subjects[0].name, "system:monitoring");
    }

    #[test]
    fn worker_bootstrapper_bindings_are_worker_scoped() {
        let objs = default_rbac_fixtures();
        for name in [
            "system:bootstrappers:node-bootstrapper",
            "system:bootstrappers:nodeclient",
        ] {
            let binding = find(&objs, "ClusterRoleBinding", name);
            let subjects = binding.subjects.as_ref().unwrap();
            assert!(
                subjects.iter().any(|s| {
                    s.kind == "Group" && s.name == "system:bootstrappers:klights:worker"
                }),
                "{name} must bind the worker bootstrap group"
            );
            assert!(
                subjects.iter().all(|s| s.name != "system:bootstrappers"),
                "{name} must not grant generic bootstrap tokens worker CSR access"
            );
        }
    }

    #[test]
    fn cluster_admin_is_bound_to_system_masters() {
        let objs = default_rbac_fixtures();
        let binding = find(&objs, "ClusterRoleBinding", "cluster-admin");
        assert_eq!(binding.role_ref.as_ref().unwrap().name, "cluster-admin");
        let subjects = binding.subjects.as_ref().unwrap();
        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].kind, "Group");
        assert_eq!(subjects[0].name, "system:masters");
    }
}
