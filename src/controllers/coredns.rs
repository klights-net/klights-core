use crate::datastore::DatastoreBackend;
use crate::kubelet::pod_repository::PodRepository;
use anyhow::Result;
use serde_json::{Value, json};
use std::sync::Arc;

const COREDNS_KUBECONFIG_PORT_ANNOTATION: &str = "klights.dev/kubeconfig-port";
const COREDNS_KUBECONFIG_PATH_ANNOTATION: &str = "klights.dev/kubeconfig-path";

/// Derive the DNS service ClusterIP from the service CIDR.
/// Returns network address + 10 (e.g., "10.43.128.0/17" -> "10.43.128.10").
pub fn derive_dns_service_ip(service_cidr: &str) -> String {
    let parts: Vec<&str> = service_cidr.split('/').collect();
    let network_addr = parts[0];
    let octets: Vec<&str> = network_addr.split('.').collect();
    let last_octet: u8 = octets[3].parse().unwrap();
    format!(
        "{}.{}.{}.{}",
        octets[0],
        octets[1],
        octets[2],
        last_octet + 10
    )
}

/// Bootstrap CoreDNS resources on startup: ConfigMap, Deployment, and Service.
/// Idempotent — skips creation if resources already exist.
pub async fn bootstrap_coredns(
    db: &dyn DatastoreBackend,
    pod_repository: Arc<PodRepository>,
    _tls_port: u16,
    service_cidr: &str,
    _containerd_namespace: &str,
    node_name: &str,
) -> Result<()> {
    let dns_ip = derive_dns_service_ip(service_cidr);

    create_coredns_serviceaccount(db).await?;
    create_coredns_rbac(db).await?;
    create_coredns_configmap(db).await?;
    create_coredns_deployment(db, pod_repository, node_name).await?;
    create_coredns_service(db, &dns_ip).await?;
    tracing::info!("CoreDNS bootstrap complete (DNS service IP: {})", dns_ip);
    Ok(())
}

async fn create_coredns_serviceaccount(db: &dyn DatastoreBackend) -> Result<()> {
    if db
        .get_resource("v1", "ServiceAccount", Some("kube-system"), "coredns")
        .await?
        .is_some()
    {
        return Ok(());
    }

    db.create_resource(
        "v1",
        "ServiceAccount",
        Some("kube-system"),
        "coredns",
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

async fn create_coredns_rbac(db: &dyn DatastoreBackend) -> Result<()> {
    create_or_reconcile_coredns_clusterrole(db).await?;
    create_or_reconcile_coredns_clusterrolebinding(db).await
}

async fn create_or_reconcile_coredns_clusterrole(db: &dyn DatastoreBackend) -> Result<()> {
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

    if let Some(existing) = db
        .get_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "system:coredns",
        )
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
        db.update_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            "system:coredns",
            updated,
            existing.resource_version,
        )
        .await?;
        tracing::info!("Updated CoreDNS ClusterRole");
        return Ok(());
    }

    db.create_resource(
        "rbac.authorization.k8s.io/v1",
        "ClusterRole",
        None,
        "system:coredns",
        desired,
    )
    .await?;
    tracing::info!("Created CoreDNS ClusterRole");
    Ok(())
}

async fn create_or_reconcile_coredns_clusterrolebinding(db: &dyn DatastoreBackend) -> Result<()> {
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

    if let Some(existing) = db
        .get_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "system:coredns",
        )
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
        db.update_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "system:coredns",
            updated,
            existing.resource_version,
        )
        .await?;
        tracing::info!("Updated CoreDNS ClusterRoleBinding");
        return Ok(());
    }

    db.create_resource(
        "rbac.authorization.k8s.io/v1",
        "ClusterRoleBinding",
        None,
        "system:coredns",
        desired,
    )
    .await?;
    tracing::info!("Created CoreDNS ClusterRoleBinding");
    Ok(())
}

async fn create_coredns_configmap(db: &dyn DatastoreBackend) -> Result<()> {
    let desired_corefile = desired_coredns_corefile();
    if let Some(existing) = db
        .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
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
        db.update_resource(
            "v1",
            "ConfigMap",
            Some("kube-system"),
            "coredns",
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

    db.create_resource("v1", "ConfigMap", Some("kube-system"), "coredns", cm)
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
    db: &dyn DatastoreBackend,
    pod_repository: Arc<PodRepository>,
    node_name: &str,
) -> Result<()> {
    if let Some(existing) = db
        .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
        .await?
    {
        let mut updated = (*existing.data).clone();
        let mut changed = remove_legacy_coredns_node_name(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_annotations(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_mount(&mut updated);
        changed |= remove_legacy_coredns_kubeconfig_volume(&mut updated);
        if changed {
            let updated = db
                .update_resource(
                    "apps/v1",
                    "Deployment",
                    Some("kube-system"),
                    "coredns",
                    updated,
                    existing.resource_version,
                )
                .await?;
            tracing::info!("Updated CoreDNS Deployment template to remove node-local kubeconfig");
            reconcile_coredns_deployment(db, pod_repository, updated, node_name).await?;
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

    let created = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("kube-system"),
            "coredns",
            deployment,
        )
        .await?;
    tracing::info!("Created CoreDNS Deployment");

    reconcile_coredns_deployment(db, pod_repository, created, node_name).await?;
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

async fn reconcile_coredns_deployment(
    db: &dyn DatastoreBackend,
    pod_repository: Arc<PodRepository>,
    deployment: crate::datastore::types::Resource,
    node_name: &str,
) -> Result<()> {
    let deployment_with_metadata =
        crate::api::inject_resource_version(deployment.data, deployment.resource_version);
    let pod_repo_ref = pod_repository.as_ref();
    crate::controllers::deployment::reconcile_deployment(
        db,
        pod_repo_ref,
        pod_repo_ref,
        pod_repo_ref,
        &deployment_with_metadata,
        node_name,
    )
    .await?;
    Ok(())
}

async fn create_coredns_service(db: &dyn DatastoreBackend, dns_ip: &str) -> Result<()> {
    let exists = db
        .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
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

    db.create_resource("v1", "Service", Some("kube-system"), "kube-dns", service)
        .await?;
    tracing::info!("Created CoreDNS Service (ClusterIP: {})", dns_ip);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bootstrap_coredns_creates_all_resources() {
        let db = crate::datastore::test_support::in_memory().await;

        // Bootstrap needs kube-system namespace
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        // Verify ConfigMap
        let cm = db
            .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
            .await
            .unwrap();
        assert!(cm.is_some(), "CoreDNS ConfigMap should exist");
        let cm_data = cm.unwrap().data;
        let corefile = cm_data["data"]["Corefile"].as_str().unwrap();
        assert!(
            corefile.contains("kubernetes cluster.local"),
            "Corefile should contain kubernetes plugin"
        );
        assert!(
            !corefile.contains("kubeconfig "),
            "CoreDNS must use in-cluster service account config, not a node-local kubeconfig"
        );

        // Verify Deployment
        let deploy = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap();
        assert!(deploy.is_some(), "CoreDNS Deployment should exist");
        let deploy_data = deploy.unwrap().data;
        assert_eq!(deploy_data["spec"]["replicas"], 1);
        assert_eq!(
            deploy_data["spec"]["template"]["spec"]["containers"][0]["image"],
            "coredns/coredns:1.11.1"
        );

        // Verify Service
        let svc = db
            .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
            .await
            .unwrap();
        assert!(svc.is_some(), "kube-dns Service should exist");
        let svc_data = svc.unwrap().data;
        assert_eq!(svc_data["spec"]["clusterIP"], "10.43.128.10");
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_creates_serviceaccount_and_rbac() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7679,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let service_account = db
            .get_resource("v1", "ServiceAccount", Some("kube-system"), "coredns")
            .await
            .unwrap();
        assert!(
            service_account.is_some(),
            "CoreDNS projected tokens must be bound to an existing kube-system/coredns ServiceAccount"
        );

        let cluster_role = db
            .get_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                "system:coredns",
            )
            .await
            .unwrap()
            .expect("CoreDNS ClusterRole must exist");
        let rules = cluster_role
            .data
            .pointer("/rules")
            .and_then(|rules| rules.as_array())
            .expect("CoreDNS ClusterRole must have rules");
        assert!(
            rules.iter().any(|rule| {
                rule["apiGroups"]
                    .as_array()
                    .is_some_and(|groups| groups.iter().any(|group| group.as_str() == Some("")))
                    && rule["resources"].as_array().is_some_and(|resources| {
                        ["endpoints", "namespaces", "pods", "services"]
                            .iter()
                            .all(|expected| {
                                resources
                                    .iter()
                                    .any(|resource| resource.as_str() == Some(*expected))
                            })
                    })
                    && rule["verbs"].as_array().is_some_and(|verbs| {
                        ["list", "watch"].iter().all(|expected| {
                            verbs.iter().any(|verb| verb.as_str() == Some(*expected))
                        })
                    })
            }),
            "CoreDNS ClusterRole must allow list/watch for core service discovery resources"
        );
        assert!(
            rules.iter().any(|rule| {
                rule["apiGroups"].as_array().is_some_and(|groups| {
                    groups
                        .iter()
                        .any(|group| group.as_str() == Some("discovery.k8s.io"))
                }) && rule["resources"].as_array().is_some_and(|resources| {
                    resources
                        .iter()
                        .any(|resource| resource.as_str() == Some("endpointslices"))
                }) && rule["verbs"].as_array().is_some_and(|verbs| {
                    ["list", "watch"]
                        .iter()
                        .all(|expected| verbs.iter().any(|verb| verb.as_str() == Some(*expected)))
                })
            }),
            "CoreDNS ClusterRole must allow list/watch for EndpointSlices"
        );

        let binding = db
            .get_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRoleBinding",
                None,
                "system:coredns",
            )
            .await
            .unwrap()
            .expect("CoreDNS ClusterRoleBinding must exist");
        assert_eq!(
            binding
                .data
                .pointer("/roleRef/name")
                .and_then(|v| v.as_str()),
            Some("system:coredns")
        );
        assert!(
            binding
                .data
                .pointer("/subjects")
                .and_then(|subjects| subjects.as_array())
                .is_some_and(|subjects| {
                    subjects.iter().any(|subject| {
                        subject["kind"].as_str() == Some("ServiceAccount")
                            && subject["name"].as_str() == Some("coredns")
                            && subject["namespace"].as_str() == Some("kube-system")
                    })
                }),
            "CoreDNS ClusterRoleBinding must bind kube-system/coredns"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_idempotent() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();
        let result = bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await;
        assert!(result.is_ok(), "Second bootstrap call should not error");

        // Should still have exactly 1 of each
        let cms = db
            .list_resources(
                "v1",
                "ConfigMap",
                Some("kube-system"),
                crate::datastore::ResourceListQuery::all(),
            )
            .await
            .unwrap();
        let coredns_cms: Vec<_> = cms.items.iter().filter(|r| r.name == "coredns").collect();
        assert_eq!(
            coredns_cms.len(),
            1,
            "Should have exactly 1 CoreDNS ConfigMap"
        );
    }

    #[tokio::test]
    async fn test_bootstrap_coredns_repairs_legacy_node_local_kubeconfig_resources() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        db.create_resource(
            "v1",
            "ConfigMap",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "data": {
                    "Corefile": ".:53 {\n  kubernetes cluster.local in-addr.arpa ip6.arpa {\n    kubeconfig /etc/coredns/kubeconfig.yaml klights-mn-controlplane1\n  }\n}\n"
                }
            }),
        )
        .await
        .unwrap();
        db.create_resource(
            "apps/v1",
            "Deployment",
            Some("kube-system"),
            "coredns",
            json!({
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "coredns", "namespace": "kube-system"},
                "spec": {
                    "replicas": 1,
                    "selector": {"matchLabels": {"k8s-app": "kube-dns"}},
                    "template": {
                        "metadata": {
                            "labels": {"k8s-app": "kube-dns"},
                            "annotations": {
                                "klights.dev/kubeconfig-port": "7679",
                                "klights.dev/kubeconfig-path": "/old/kubeconfig.yaml"
                            }
                        },
                        "spec": {
                            "nodeName": "mn-controlplane1",
                            "containers": [{
                                "name": "coredns",
                                "image": "coredns/coredns:1.11.1",
                                "args": ["-conf", "/etc/coredns/Corefile"],
                                "volumeMounts": [
                                    {"name": "config-volume", "mountPath": "/etc/coredns/Corefile", "subPath": "Corefile", "readOnly": true},
                                    {"name": "kubeconfig", "mountPath": "/etc/coredns/kubeconfig.yaml", "subPath": "kubeconfig.yaml", "readOnly": true}
                                ]
                            }],
                            "volumes": [
                                {"name": "config-volume", "configMap": {"name": "coredns"}},
                                {"name": "kubeconfig", "hostPath": {"path": "/old/kubeconfig.yaml", "type": "File"}}
                            ],
                            "dnsPolicy": "Default"
                        }
                    }
                }
            }),
        )
        .await
        .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7679,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let updated = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();
        let cm = db
            .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();
        let corefile = cm.data["data"]["Corefile"].as_str().unwrap();
        assert!(
            !corefile.contains("kubeconfig "),
            "bootstrap must repair stale node-local CoreDNS kubeconfig directives"
        );
        assert!(
            updated
                .data
                .pointer("/spec/template/spec/nodeName")
                .is_none(),
            "CoreDNS must not stay pinned to the bootstrap node"
        );
        let volume_mounts =
            updated.data["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
                .as_array()
                .unwrap();
        assert!(
            volume_mounts
                .iter()
                .all(|vm| vm["name"].as_str() != Some("kubeconfig")),
            "CoreDNS must not mount a stale node-local kubeconfig"
        );
        let volumes = updated.data["spec"]["template"]["spec"]["volumes"]
            .as_array()
            .unwrap();
        assert!(
            volumes
                .iter()
                .all(|volume| volume["name"].as_str() != Some("kubeconfig")),
            "CoreDNS must not keep the stale kubeconfig hostPath volume"
        );
    }

    #[test]
    fn test_derive_dns_service_ip_from_service_cidr() {
        let test_cases = vec![
            ("10.43.128.0/17", "10.43.128.10"),
            ("10.50.128.0/17", "10.50.128.10"),
            ("192.168.0.0/24", "192.168.0.10"),
            ("172.16.0.0/16", "172.16.0.10"),
        ];

        for (cidr, expected_ip) in test_cases {
            let result = derive_dns_service_ip(cidr);
            assert_eq!(
                result, expected_ip,
                "CIDR {} should yield DNS IP {}",
                cidr, expected_ip
            );
        }
    }

    #[tokio::test]
    async fn test_coredns_service_uses_derived_ip_from_custom_cidr() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        let custom_service_cidr = "10.50.128.0/17";
        let expected_dns_ip = "10.50.128.10";

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            custom_service_cidr,
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let svc = db
            .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
            .await
            .unwrap();
        assert!(svc.is_some(), "kube-dns Service should exist");
        let svc_data = svc.unwrap().data;
        assert_eq!(
            svc_data["spec"]["clusterIP"], expected_dns_ip,
            "Service ClusterIP should match derived DNS IP"
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_has_dns_policy_default() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let deploy = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();

        let dns_policy = deploy
            .data
            .pointer("/spec/template/spec/dnsPolicy")
            .and_then(|v| v.as_str());
        assert_eq!(
            dns_policy,
            Some("Default"),
            "CoreDNS must use dnsPolicy: Default to avoid DNS loop"
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_template_is_not_pinned_to_bootstrap_node() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "bootstrap-node",
        )
        .await
        .unwrap();

        let deploy = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();

        assert!(
            deploy
                .data
                .pointer("/spec/template/spec/nodeName")
                .is_none(),
            "CoreDNS Deployment must remain scheduler-bindable after bootstrap node loss"
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_volume_mounts() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let deploy = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();

        let container = &deploy.data["spec"]["template"]["spec"]["containers"][0];
        let volume_mounts = container["volumeMounts"].as_array().unwrap();

        // Verify Corefile mount
        let corefile_mount = volume_mounts
            .iter()
            .find(|vm| vm["mountPath"].as_str() == Some("/etc/coredns/Corefile"));
        assert!(
            corefile_mount.is_some(),
            "Must mount Corefile at /etc/coredns/Corefile"
        );
        assert_eq!(
            corefile_mount.unwrap()["subPath"].as_str(),
            Some("Corefile"),
            "Corefile mount must use subPath"
        );

        // Verify there is no node-local kubeconfig mount. CoreDNS must use its
        // projected ServiceAccount token and the kubernetes Service instead.
        let kubeconfig_mount = volume_mounts
            .iter()
            .find(|vm| vm["mountPath"].as_str() == Some("/etc/coredns/kubeconfig.yaml"));
        assert!(
            kubeconfig_mount.is_none(),
            "Must not mount a node-local kubeconfig at /etc/coredns/kubeconfig.yaml"
        );
    }

    #[tokio::test]
    async fn test_coredns_deployment_labels() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let deploy = db
            .get_resource("apps/v1", "Deployment", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();

        // Deployment metadata labels
        assert_eq!(
            deploy.data["metadata"]["labels"]["k8s-app"].as_str(),
            Some("kube-dns"),
            "Deployment must have k8s-app=kube-dns label"
        );

        // Pod template labels must match selector
        let selector_labels = &deploy.data["spec"]["selector"]["matchLabels"];
        let template_labels = &deploy.data["spec"]["template"]["metadata"]["labels"];
        assert_eq!(
            selector_labels, template_labels,
            "Selector matchLabels must match template labels"
        );
    }

    #[tokio::test]
    async fn test_coredns_service_cluster_ips_array() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let svc = db
            .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
            .await
            .unwrap()
            .unwrap();

        let cluster_ip = svc.data["spec"]["clusterIP"].as_str().unwrap();
        let cluster_ips = svc.data["spec"]["clusterIPs"].as_array().unwrap();

        assert_eq!(cluster_ips.len(), 1);
        assert_eq!(
            cluster_ips[0].as_str().unwrap(),
            cluster_ip,
            "clusterIPs[0] must match clusterIP"
        );
    }

    #[tokio::test]
    async fn test_coredns_service_ports() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights",
            "test-node",
        )
        .await
        .unwrap();

        let svc = db
            .get_resource("v1", "Service", Some("kube-system"), "kube-dns")
            .await
            .unwrap()
            .unwrap();

        let ports = svc.data["spec"]["ports"].as_array().unwrap();
        assert_eq!(ports.len(), 2, "kube-dns must expose UDP and TCP port 53");

        let udp_port = ports.iter().find(|p| p["protocol"].as_str() == Some("UDP"));
        assert!(udp_port.is_some(), "Must have UDP port");
        assert_eq!(udp_port.unwrap()["port"].as_i64(), Some(53));

        let tcp_port = ports.iter().find(|p| p["protocol"].as_str() == Some("TCP"));
        assert!(tcp_port.is_some(), "Must have TCP port");
        assert_eq!(tcp_port.unwrap()["port"].as_i64(), Some(53));
    }

    #[tokio::test]
    async fn test_coredns_configmap_namespace_in_corefile() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::controllers::namespace::init_default_namespaces(&db)
            .await
            .unwrap();

        // Use a custom containerd namespace
        bootstrap_coredns(
            &db,
            crate::controllers::test_utils::pod_repository_for_test(&db),
            7443,
            "10.43.128.0/17",
            "klights-architect",
            "test-node",
        )
        .await
        .unwrap();

        let cm = db
            .get_resource("v1", "ConfigMap", Some("kube-system"), "coredns")
            .await
            .unwrap()
            .unwrap();

        let corefile = cm.data["data"]["Corefile"].as_str().unwrap();
        assert!(
            !corefile.contains("kubeconfig "),
            "Corefile must not reference a containerd namespace as kubeconfig context"
        );
    }
}
