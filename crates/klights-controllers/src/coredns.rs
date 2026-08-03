use anyhow::Result;
use async_trait::async_trait;
use klights_cluster_core::Resource;
use klights_reconcile_api::ControllerStoreResult;
use serde_json::{Value, json};

const COREDNS_KUBECONFIG_PORT_ANNOTATION: &str = "klights.dev/kubeconfig-port";
const COREDNS_KUBECONFIG_PATH_ANNOTATION: &str = "klights.dev/kubeconfig-path";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoreDnsResourceKind {
    ServiceAccount,
    ClusterRole,
    ClusterRoleBinding,
    ConfigMap,
    Deployment,
    Service,
}

impl CoreDnsResourceKind {
    /// Return the canonical Kubernetes identity for this CoreDNS resource.
    pub fn coordinates(
        self,
    ) -> (
        &'static str,
        &'static str,
        Option<&'static str>,
        &'static str,
    ) {
        match self {
            Self::ServiceAccount => ("v1", "ServiceAccount", Some("kube-system"), "coredns"),
            Self::ClusterRole => (
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                "system:coredns",
            ),
            Self::ClusterRoleBinding => (
                "rbac.authorization.k8s.io/v1",
                "ClusterRoleBinding",
                None,
                "system:coredns",
            ),
            Self::ConfigMap => ("v1", "ConfigMap", Some("kube-system"), "coredns"),
            Self::Deployment => ("apps/v1", "Deployment", Some("kube-system"), "coredns"),
            Self::Service => ("v1", "Service", Some("kube-system"), "kube-dns"),
        }
    }
}

#[async_trait]
pub trait CoreDnsBootstrapStore: Send + Sync {
    async fn get_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
    ) -> ControllerStoreResult<Option<Resource>>;
    async fn create_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
    ) -> ControllerStoreResult<Resource>;
    async fn update_coredns_resource(
        &self,
        kind: CoreDnsResourceKind,
        value: Value,
        expected_resource_version: i64,
    ) -> ControllerStoreResult<Resource>;
    async fn reconcile_coredns_deployment(
        &self,
        deployment: Resource,
        node_name: &str,
    ) -> ControllerStoreResult<()>;
}

/// Derive the DNS service ClusterIP from the service CIDR.
/// Returns network address + 10 (e.g., "10.43.128.0/17" -> "10.43.128.10").
pub fn derive_dns_service_ip(service_cidr: &str) -> String {
    klights_types::dns_service_ipv4(service_cidr)
}

pub async fn bootstrap_coredns_with_store(
    store: &dyn CoreDnsBootstrapStore,
    _tls_port: u16,
    service_cidr: &str,
    _containerd_namespace: &str,
    node_name: &str,
) -> Result<()> {
    let dns_ip = derive_dns_service_ip(service_cidr);

    create_coredns_serviceaccount(store).await?;
    create_coredns_rbac(store).await?;
    create_coredns_configmap(store).await?;
    create_coredns_deployment(store, node_name).await?;
    create_coredns_service(store, &dns_ip).await?;
    tracing::info!("CoreDNS bootstrap complete (DNS service IP: {})", dns_ip);
    Ok(())
}

async fn create_coredns_serviceaccount(store: &dyn CoreDnsBootstrapStore) -> Result<()> {
    if store
        .get_coredns_resource(CoreDnsResourceKind::ServiceAccount)
        .await?
        .is_some()
    {
        return Ok(());
    }

    store
        .create_coredns_resource(
            CoreDnsResourceKind::ServiceAccount,
            json!({
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": "coredns",
                "namespace": "kube-system",
                "labels": {
                    "k8s-app": "kube-dns",
                    "kubernetes.io/name": "CoreDNS"
                }
            }
            }),
        )
        .await?;
    tracing::info!("Created CoreDNS ServiceAccount");
    Ok(())
}

async fn create_coredns_rbac(store: &dyn CoreDnsBootstrapStore) -> Result<()> {
    create_or_reconcile_coredns_clusterrole(store).await?;
    create_or_reconcile_coredns_clusterrolebinding(store).await
}

async fn create_or_reconcile_coredns_clusterrole(store: &dyn CoreDnsBootstrapStore) -> Result<()> {
    let desired = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRole",
        "metadata": {
            "name": "system:coredns",
            "labels": {
                "k8s-app": "kube-dns",
                "kubernetes.io/name": "CoreDNS"
            }
        },
        "rules": [
            {
                "apiGroups": [""],
                "resources": ["endpoints", "namespaces", "pods", "services"],
                "verbs": ["list", "watch"]
            },
            {
                "apiGroups": ["discovery.k8s.io"],
                "resources": ["endpointslices"],
                "verbs": ["list", "watch"]
            }
        ]
    });

    if let Some(existing) = store
        .get_coredns_resource(CoreDnsResourceKind::ClusterRole)
        .await?
    {
        if existing.data.pointer("/rules") == desired.pointer("/rules") {
            return Ok(());
        }

        let mut updated = (*existing.data).clone();
        updated
            .as_object_mut()
            .expect("ClusterRole resource must be a JSON object")
            .insert("rules".to_string(), desired["rules"].clone());
        store
            .update_coredns_resource(
                CoreDnsResourceKind::ClusterRole,
                updated,
                existing.resource_version,
            )
            .await?;
        tracing::info!("Updated CoreDNS ClusterRole");
        return Ok(());
    }

    store
        .create_coredns_resource(CoreDnsResourceKind::ClusterRole, desired)
        .await?;
    tracing::info!("Created CoreDNS ClusterRole");
    Ok(())
}

async fn create_or_reconcile_coredns_clusterrolebinding(
    store: &dyn CoreDnsBootstrapStore,
) -> Result<()> {
    let desired = json!({
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {
            "name": "system:coredns",
            "labels": {
                "k8s-app": "kube-dns",
                "kubernetes.io/name": "CoreDNS"
            }
        },
        "roleRef": {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "system:coredns"
        },
        "subjects": [
            {
                "kind": "ServiceAccount",
                "name": "coredns",
                "namespace": "kube-system"
            }
        ]
    });

    if let Some(existing) = store
        .get_coredns_resource(CoreDnsResourceKind::ClusterRoleBinding)
        .await?
    {
        if existing.data.pointer("/roleRef") == desired.pointer("/roleRef")
            && existing.data.pointer("/subjects") == desired.pointer("/subjects")
        {
            return Ok(());
        }

        let mut updated = (*existing.data).clone();
        let object = updated
            .as_object_mut()
            .expect("ClusterRoleBinding resource must be a JSON object");
        object.insert("roleRef".to_string(), desired["roleRef"].clone());
        object.insert("subjects".to_string(), desired["subjects"].clone());
        store
            .update_coredns_resource(
                CoreDnsResourceKind::ClusterRoleBinding,
                updated,
                existing.resource_version,
            )
            .await?;
        tracing::info!("Updated CoreDNS ClusterRoleBinding");
        return Ok(());
    }

    store
        .create_coredns_resource(CoreDnsResourceKind::ClusterRoleBinding, desired)
        .await?;
    tracing::info!("Created CoreDNS ClusterRoleBinding");
    Ok(())
}

async fn create_coredns_configmap(store: &dyn CoreDnsBootstrapStore) -> Result<()> {
    let desired_corefile = desired_coredns_corefile();
    if let Some(existing) = store
        .get_coredns_resource(CoreDnsResourceKind::ConfigMap)
        .await?
    {
        let current = existing
            .data
            .pointer("/data/Corefile")
            .and_then(|value| value.as_str());
        if current == Some(desired_corefile.as_str()) {
            return Ok(());
        }

        let mut updated = (*existing.data).clone();
        let data = updated
            .as_object_mut()
            .expect("ConfigMap resource must be a JSON object")
            .entry("data".to_string())
            .or_insert_with(|| json!({}));
        let data = data
            .as_object_mut()
            .expect("ConfigMap data must be a JSON object");
        data.insert("Corefile".to_string(), json!(desired_corefile));
        store
            .update_coredns_resource(
                CoreDnsResourceKind::ConfigMap,
                updated,
                existing.resource_version,
            )
            .await?;
        tracing::info!("Updated CoreDNS ConfigMap to in-cluster API config");
        return Ok(());
    }

    let cm = json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system"
        },
        "data": {
            "Corefile": desired_corefile
        }
    });

    store
        .create_coredns_resource(CoreDnsResourceKind::ConfigMap, cm)
        .await?;
    tracing::info!("Created CoreDNS ConfigMap");
    Ok(())
}

fn desired_coredns_corefile() -> String {
    r#".:53 {
    errors
    health
    ready
    kubernetes cluster.local in-addr.arpa ip6.arpa {
      pods insecure
      fallthrough in-addr.arpa ip6.arpa
    }
    forward . /etc/resolv.conf
    cache 30
    loop
    reload
    loadbalance
}
"#
    .to_string()
}

async fn create_coredns_deployment(
    store: &dyn CoreDnsBootstrapStore,
    node_name: &str,
) -> Result<()> {
    if let Some(existing) = store
        .get_coredns_resource(CoreDnsResourceKind::Deployment)
        .await?
    {
        let mut updated = (*existing.data).clone();
        let mut changed = remove_legacy_coredns_node_name(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_annotations(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_mount(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_volume(&mut updated);
        if changed {
            let updated = store
                .update_coredns_resource(
                    CoreDnsResourceKind::Deployment,
                    updated,
                    existing.resource_version,
                )
                .await?;
            tracing::info!("Updated CoreDNS Deployment template to remove node-local kubeconfig");
            store
                .reconcile_coredns_deployment(updated, node_name)
                .await?;
        }
        return Ok(());
    }

    let deployment = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "coredns",
            "namespace": "kube-system",
            "labels": {
                "k8s-app": "kube-dns",
                "kubernetes.io/name": "CoreDNS"
            }
        },
        "spec": {
            "replicas": 1,
            "selector": {
                "matchLabels": {
                    "k8s-app": "kube-dns"
                }
            },
            "template": {
                "metadata": {
                    "labels": {
                        "k8s-app": "kube-dns"
                    }
                },
                "spec": {
                    "serviceAccountName": "coredns",
                    "containers": [{
                        "name": "coredns",
                        "image": "coredns/coredns:1.11.1",
                        "args": ["-conf", "/etc/coredns/Corefile"],
                        "ports": [
                            {"containerPort": 53, "name": "dns", "protocol": "UDP"},
                            {"containerPort": 53, "name": "dns-tcp", "protocol": "TCP"}
                        ],
                        "volumeMounts": [
                            {
                                "name": "config-volume",
                                "mountPath": "/etc/coredns/Corefile",
                                "subPath": "Corefile",
                                "readOnly": true
                            }
                        ]
                    }],
                    "volumes": [
                        {
                            "name": "config-volume",
                            "configMap": {
                                "name": "coredns"
                            }
                        }
                    ],
                    "dnsPolicy": "Default"
                }
            }
        }
    });

    let created = store
        .create_coredns_resource(CoreDnsResourceKind::Deployment, deployment)
        .await?;
    tracing::info!("Created CoreDNS Deployment");

    store
        .reconcile_coredns_deployment(created, node_name)
        .await?;
    tracing::info!("Reconciled CoreDNS Deployment (ReplicaSet + Pod created)");
    Ok(())
}

fn remove_legacy_coredns_node_name(deployment: &mut Value) -> bool {
    deployment
        .pointer_mut("/spec/template/spec")
        .and_then(|spec| spec.as_object_mut())
        .is_some_and(|spec| spec.remove("nodeName").is_some())
}

fn remove_legacy_coredns_kubeconfig_annotations(deployment: &mut Value) -> bool {
    let Some(annotations) = deployment
        .pointer_mut("/spec/template/metadata/annotations")
        .and_then(|annotations| annotations.as_object_mut())
    else {
        return false;
    };
    let mut changed = false;
    changed |= annotations
        .remove(COREDNS_KUBECONFIG_PORT_ANNOTATION)
        .is_some();
    changed |= annotations
        .remove(COREDNS_KUBECONFIG_PATH_ANNOTATION)
        .is_some();
    changed
}

fn remove_legacy_coredns_kubeconfig_mount(deployment: &mut Value) -> bool {
    remove_array_entries_by_name(
        deployment.pointer_mut("/spec/template/spec/containers/0/volumeMounts"),
        "kubeconfig",
    )
}

fn remove_legacy_coredns_kubeconfig_volume(deployment: &mut Value) -> bool {
    remove_array_entries_by_name(
        deployment.pointer_mut("/spec/template/spec/volumes"),
        "kubeconfig",
    )
}

fn remove_array_entries_by_name(value: Option<&mut Value>, name: &str) -> bool {
    let Some(items) = value.and_then(|value| value.as_array_mut()) else {
        return false;
    };
    let before = items.len();
    items.retain(|item| item.get("name").and_then(|value| value.as_str()) != Some(name));
    items.len() != before
}

async fn create_coredns_service(store: &dyn CoreDnsBootstrapStore, dns_ip: &str) -> Result<()> {
    let exists = store
        .get_coredns_resource(CoreDnsResourceKind::Service)
        .await?
        .is_some();
    if exists {
        return Ok(());
    }

    let service = json!({
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": "kube-dns",
            "namespace": "kube-system",
            "labels": {
                "k8s-app": "kube-dns",
                "kubernetes.io/cluster-service": "true",
                "kubernetes.io/name": "CoreDNS"
            }
        },
        "spec": {
            "selector": {
                "k8s-app": "kube-dns"
            },
            "clusterIP": dns_ip,
            "clusterIPs": [dns_ip],
            "ports": [
                {"name": "dns", "port": 53, "protocol": "UDP"},
                {"name": "dns-tcp", "port": 53, "protocol": "TCP"}
            ]
        }
    });

    store
        .create_coredns_resource(CoreDnsResourceKind::Service, service)
        .await?;
    tracing::info!("Created CoreDNS Service (ClusterIP: {})", dns_ip);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use klights_reconcile_api::{ControllerStoreError, ControllerStoreResult};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryCoreDnsStore {
        resources: Mutex<Vec<(CoreDnsResourceKind, Resource)>>,
        created: Mutex<Vec<CoreDnsResourceKind>>,
        reconciled_deployments: Mutex<usize>,
    }

    impl MemoryCoreDnsStore {
        fn resource(&self, kind: CoreDnsResourceKind) -> Option<Resource> {
            self.resources
                .lock()
                .unwrap()
                .iter()
                .find_map(|(candidate, resource)| (*candidate == kind).then(|| resource.clone()))
        }

        fn seed(&self, kind: CoreDnsResourceKind, value: Value) {
            let resource = resource_with_version(value, 1);
            self.resources.lock().unwrap().push((kind, resource));
        }
    }

    fn resource_with_version(mut value: Value, resource_version: i64) -> Resource {
        let metadata = value
            .get_mut("metadata")
            .and_then(Value::as_object_mut)
            .expect("CoreDNS test resource metadata");
        metadata
            .entry("uid".to_string())
            .or_insert_with(|| json!(format!("test-{resource_version}")));
        metadata.insert(
            "resourceVersion".to_string(),
            json!(resource_version.to_string()),
        );
        Resource::try_from_data(Arc::new(value)).expect("CoreDNS test resource identity")
    }

    #[async_trait]
    impl CoreDnsBootstrapStore for MemoryCoreDnsStore {
        async fn get_coredns_resource(
            &self,
            kind: CoreDnsResourceKind,
        ) -> ControllerStoreResult<Option<Resource>> {
            Ok(self.resource(kind))
        }

        async fn create_coredns_resource(
            &self,
            kind: CoreDnsResourceKind,
            value: Value,
        ) -> ControllerStoreResult<Resource> {
            if self.resource(kind).is_some() {
                return Err(ControllerStoreError::conflict("duplicate CoreDNS resource"));
            }
            let resource_version = self.resources.lock().unwrap().len() as i64 + 1;
            let resource = resource_with_version(value, resource_version);
            self.resources
                .lock()
                .unwrap()
                .push((kind, resource.clone()));
            self.created.lock().unwrap().push(kind);
            Ok(resource)
        }

        async fn update_coredns_resource(
            &self,
            kind: CoreDnsResourceKind,
            value: Value,
            expected_resource_version: i64,
        ) -> ControllerStoreResult<Resource> {
            let mut resources = self.resources.lock().unwrap();
            let Some((_, current)) = resources
                .iter_mut()
                .find(|(candidate, _)| *candidate == kind)
            else {
                return Err(ControllerStoreError::not_found("CoreDNS resource missing"));
            };
            if current.resource_version != expected_resource_version {
                return Err(ControllerStoreError::conflict("stale CoreDNS resource"));
            }
            let updated = resource_with_version(value, expected_resource_version + 1);
            *current = updated.clone();
            Ok(updated)
        }

        async fn reconcile_coredns_deployment(
            &self,
            _deployment: Resource,
            _node_name: &str,
        ) -> ControllerStoreResult<()> {
            *self.reconciled_deployments.lock().unwrap() += 1;
            Ok(())
        }
    }

    async fn bootstrap(store: &MemoryCoreDnsStore, service_cidr: &str, node_name: &str) {
        bootstrap_coredns_with_store(store, 7443, service_cidr, "klights", node_name)
            .await
            .unwrap();
    }

    fn data(store: &MemoryCoreDnsStore, kind: CoreDnsResourceKind) -> Arc<Value> {
        store.resource(kind).expect("CoreDNS resource").data
    }

    #[test]
    fn resource_kinds_map_to_exact_kubernetes_identities() {
        let cases = [
            (
                CoreDnsResourceKind::ServiceAccount,
                ("v1", "ServiceAccount", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::ClusterRole,
                (
                    "rbac.authorization.k8s.io/v1",
                    "ClusterRole",
                    None,
                    "system:coredns",
                ),
            ),
            (
                CoreDnsResourceKind::ClusterRoleBinding,
                (
                    "rbac.authorization.k8s.io/v1",
                    "ClusterRoleBinding",
                    None,
                    "system:coredns",
                ),
            ),
            (
                CoreDnsResourceKind::ConfigMap,
                ("v1", "ConfigMap", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::Deployment,
                ("apps/v1", "Deployment", Some("kube-system"), "coredns"),
            ),
            (
                CoreDnsResourceKind::Service,
                ("v1", "Service", Some("kube-system"), "kube-dns"),
            ),
        ];

        for (kind, expected) in cases {
            assert_eq!(kind.coordinates(), expected);
        }
    }

    #[tokio::test]
    async fn focused_store_bootstrap_creates_exact_resource_family_once() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "node-a").await;
        assert_eq!(
            *store.created.lock().unwrap(),
            vec![
                CoreDnsResourceKind::ServiceAccount,
                CoreDnsResourceKind::ClusterRole,
                CoreDnsResourceKind::ClusterRoleBinding,
                CoreDnsResourceKind::ConfigMap,
                CoreDnsResourceKind::Deployment,
                CoreDnsResourceKind::Service,
            ]
        );
        assert_eq!(*store.reconciled_deployments.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_creates_all_resources() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;

        let configmap = data(&store, CoreDnsResourceKind::ConfigMap);
        let corefile = configmap["data"]["Corefile"].as_str().unwrap();
        assert!(corefile.contains("kubernetes cluster.local"));
        assert!(!corefile.contains("kubeconfig "));

        let deployment = data(&store, CoreDnsResourceKind::Deployment);
        assert_eq!(deployment["spec"]["replicas"], 1);
        assert_eq!(
            deployment["spec"]["template"]["spec"]["containers"][0]["image"],
            "coredns/coredns:1.11.1"
        );

        let service = data(&store, CoreDnsResourceKind::Service);
        assert_eq!(service["spec"]["clusterIP"], "10.43.128.10");
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_creates_serviceaccount_and_rbac() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;

        assert!(
            store
                .resource(CoreDnsResourceKind::ServiceAccount)
                .is_some()
        );
        let role = data(&store, CoreDnsResourceKind::ClusterRole);
        let rules = role["rules"].as_array().unwrap();
        assert!(rules.iter().any(|rule| {
            rule["apiGroups"]
                .as_array()
                .is_some_and(|groups| groups.iter().any(|group| group.as_str() == Some("")))
                && rule["resources"].as_array().is_some_and(|resources| {
                    ["endpoints", "namespaces", "pods", "services"]
                        .iter()
                        .all(|expected| {
                            resources.iter().any(|item| item.as_str() == Some(expected))
                        })
                })
                && rule["verbs"].as_array().is_some_and(|verbs| {
                    ["list", "watch"]
                        .iter()
                        .all(|expected| verbs.iter().any(|item| item.as_str() == Some(expected)))
                })
        }));
        assert!(rules.iter().any(|rule| {
            rule["apiGroups"].as_array().is_some_and(|groups| {
                groups
                    .iter()
                    .any(|group| group.as_str() == Some("discovery.k8s.io"))
            }) && rule["resources"].as_array().is_some_and(|resources| {
                resources
                    .iter()
                    .any(|item| item.as_str() == Some("endpointslices"))
            })
        }));

        let binding = data(&store, CoreDnsResourceKind::ClusterRoleBinding);
        assert_eq!(
            binding.pointer("/roleRef/name").and_then(Value::as_str),
            Some("system:coredns")
        );
        assert!(binding["subjects"].as_array().is_some_and(|subjects| {
            subjects.iter().any(|subject| {
                subject["kind"] == "ServiceAccount"
                    && subject["name"] == "coredns"
                    && subject["namespace"] == "kube-system"
            })
        }));
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_idempotent() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        bootstrap(&store, "10.43.128.0/17", "test-node").await;

        assert_eq!(store.created.lock().unwrap().len(), 6);
        assert_eq!(
            store
                .resources
                .lock()
                .unwrap()
                .iter()
                .filter(|(kind, _)| *kind == CoreDnsResourceKind::ConfigMap)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_repairs_legacy_node_local_kubeconfig_resources() {
        let store = MemoryCoreDnsStore::default();
        store.seed(
            CoreDnsResourceKind::ConfigMap,
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "data": {"Corefile": ".:53 {\n kubernetes cluster.local {\n  kubeconfig /etc/coredns/kubeconfig.yaml old\n }\n}\n"}
            }),
        );
        store.seed(
            CoreDnsResourceKind::Deployment,
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"k8s-app": "kube-dns"}},
                    "template": {
                        "metadata": {"labels": {"k8s-app": "kube-dns"}},
                        "spec": {
                            "nodeName": "old-node",
                            "containers": [{
                                "name": "coredns",
                                "image": "coredns/coredns:1.11.1",
                                "volumeMounts": [
                                    {"name": "config-volume", "mountPath": "/etc/coredns/Corefile"},
                                    {"name": "kubeconfig", "mountPath": "/etc/coredns/kubeconfig.yaml"}
                                ]
                            }],
                            "volumes": [
                                {"name": "config-volume", "configMap": {"name": "coredns"}},
                                {"name": "kubeconfig", "hostPath": {"path": "/old/kubeconfig.yaml"}}
                            ]
                        }
                    }
                }
            }),
        );

        bootstrap(&store, "10.43.128.0/17", "test-node").await;

        let configmap = data(&store, CoreDnsResourceKind::ConfigMap);
        assert!(
            !configmap["data"]["Corefile"]
                .as_str()
                .unwrap()
                .contains("kubeconfig ")
        );
        let deployment = data(&store, CoreDnsResourceKind::Deployment);
        assert!(deployment.pointer("/spec/template/spec/nodeName").is_none());
        assert!(
            deployment["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
                .as_array()
                .unwrap()
                .iter()
                .all(|mount| mount["name"] != "kubeconfig")
        );
        assert!(
            deployment["spec"]["template"]["spec"]["volumes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|volume| volume["name"] != "kubeconfig")
        );
    }

    #[test]
    fn test_derive_dns_service_ip_from_service_cidr() {
        for (cidr, expected) in [
            ("10.43.128.0/17", "10.43.128.10"),
            ("10.50.128.0/17", "10.50.128.10"),
            ("192.168.0.0/24", "192.168.0.10"),
            ("172.16.0.0/16", "172.16.0.10"),
        ] {
            assert_eq!(derive_dns_service_ip(cidr), expected);
        }
    }

    #[tokio::test]
    async fn test_coredns_service_uses_derived_ip_from_custom_cidr() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.50.128.0/17", "test-node").await;
        assert_eq!(
            data(&store, CoreDnsResourceKind::Service)["spec"]["clusterIP"],
            "10.50.128.10"
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_has_dns_policy_default() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        assert_eq!(
            data(&store, CoreDnsResourceKind::Deployment)
                .pointer("/spec/template/spec/dnsPolicy")
                .and_then(Value::as_str),
            Some("Default")
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_template_is_not_pinned_to_bootstrap_node() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "bootstrap-node").await;
        assert!(
            data(&store, CoreDnsResourceKind::Deployment)
                .pointer("/spec/template/spec/nodeName")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_volume_mounts() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        let deployment = data(&store, CoreDnsResourceKind::Deployment);
        let mounts = deployment["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
            .as_array()
            .unwrap();
        let corefile = mounts
            .iter()
            .find(|mount| mount["mountPath"] == "/etc/coredns/Corefile")
            .expect("Corefile mount");
        assert_eq!(corefile["subPath"], "Corefile");
        assert!(
            mounts
                .iter()
                .all(|mount| mount["mountPath"] != "/etc/coredns/kubeconfig.yaml")
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_labels() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        let deployment = data(&store, CoreDnsResourceKind::Deployment);
        assert_eq!(deployment["metadata"]["labels"]["k8s-app"], "kube-dns");
        assert_eq!(
            deployment["spec"]["selector"]["matchLabels"],
            deployment["spec"]["template"]["metadata"]["labels"]
        );
    }

    #[tokio::test]
    async fn test_coredns_service_cluster_ips_array() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        let service = data(&store, CoreDnsResourceKind::Service);
        assert_eq!(service["spec"]["clusterIPs"].as_array().unwrap().len(), 1);
        assert_eq!(
            service["spec"]["clusterIPs"][0],
            service["spec"]["clusterIP"]
        );
    }

    #[tokio::test]
    async fn test_coredns_service_ports() {
        let store = MemoryCoreDnsStore::default();
        bootstrap(&store, "10.43.128.0/17", "test-node").await;
        let service = data(&store, CoreDnsResourceKind::Service);
        let ports = service["spec"]["ports"].as_array().unwrap();
        assert_eq!(ports.len(), 2);
        for protocol in ["UDP", "TCP"] {
            let port = ports
                .iter()
                .find(|port| port["protocol"] == protocol)
                .expect("DNS service protocol");
            assert_eq!(port["port"], 53);
        }
    }

    #[tokio::test]
    async fn test_coredns_configmap_namespace_in_corefile() {
        let store = MemoryCoreDnsStore::default();
        bootstrap_coredns_with_store(
            &store,
            7443,
            "10.43.128.0/17",
            "klights-architect",
            "test-node",
        )
        .await
        .unwrap();
        let configmap = data(&store, CoreDnsResourceKind::ConfigMap);
        assert!(
            !configmap["data"]["Corefile"]
                .as_str()
                .unwrap()
                .contains("kubeconfig ")
        );
    }
}
