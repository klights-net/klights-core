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
