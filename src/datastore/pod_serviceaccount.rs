use serde_json::{Value, json};

use crate::datastore::backend::DatastoreBackend;

const SERVICEACCOUNT_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// Auto-inject the Kubernetes ServiceAccount projected volume into a Pod spec.
///
/// This is intentionally idempotent because direct SQLite creates and raft
/// creates can both pass through backend layers that historically performed
/// this defaulting.
pub fn inject_serviceaccount_volume(pod: &mut Value) {
    let automount = pod
        .pointer("/spec/automountServiceAccountToken")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    if !automount {
        return;
    }

    let existing_volume_name = default_serviceaccount_projected_volume_name(pod);
    let volume_name = existing_volume_name.unwrap_or_else(generate_serviceaccount_volume_name);

    if default_serviceaccount_projected_volume_name(pod).is_none()
        && let Some(spec_obj) = pod.pointer_mut("/spec").and_then(|s| s.as_object_mut())
    {
        let projected_volume = json!({
            "name": volume_name.clone(),
            "projected": {
                "defaultMode": 420,
                "sources": [
                    {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                    {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                    {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}}
                ]
            }
        });
        let volumes = spec_obj
            .entry("volumes".to_string())
            .or_insert_with(|| json!([]));
        if let Some(volumes_arr) = volumes.as_array_mut() {
            volumes_arr.push(projected_volume);
        }
    }

    inject_serviceaccount_mounts(pod, &volume_name, "/spec/initContainers");
    inject_serviceaccount_mounts(pod, &volume_name, "/spec/containers");
}

pub async fn should_inject_serviceaccount_volume<B: DatastoreBackend + ?Sized>(
    db: &B,
    pod: &Value,
    namespace: Option<&str>,
) -> bool {
    if let Some(pod_automount) = pod
        .pointer("/spec/automountServiceAccountToken")
        .and_then(|v| v.as_bool())
    {
        return pod_automount;
    }

    let ns = namespace.unwrap_or("default");
    let sa_name = pod
        .pointer("/spec/serviceAccountName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("default");

    match db
        .get_resource("v1", "ServiceAccount", Some(ns), sa_name)
        .await
    {
        Ok(Some(sa)) => sa
            .data
            .pointer("/automountServiceAccountToken")
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        _ => true,
    }
}

fn generate_serviceaccount_volume_name() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    uuid::Uuid::new_v4().hash(&mut hasher);
    let hash = hasher.finish();
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let suffix: String = (0..5)
        .map(|i| CHARS[((hash >> (i * 6)) as usize) % CHARS.len()] as char)
        .collect();
    format!("kube-api-access-{suffix}")
}

fn default_serviceaccount_projected_volume_name(pod: &Value) -> Option<String> {
    pod.pointer("/spec/volumes")
        .and_then(|value| value.as_array())
        .and_then(|volumes| {
            volumes.iter().find_map(|volume| {
                let name = volume.get("name").and_then(|value| value.as_str())?;
                (name.starts_with("kube-api-access-")
                    && has_default_serviceaccount_projected_sources(volume))
                .then(|| name.to_string())
            })
        })
}

fn has_default_serviceaccount_projected_sources(volume: &Value) -> bool {
    let Some(sources) = volume
        .pointer("/projected/sources")
        .and_then(|value| value.as_array())
    else {
        return false;
    };

    let has_token = sources.iter().any(|source| {
        source
            .pointer("/serviceAccountToken/path")
            .and_then(|value| value.as_str())
            == Some("token")
    });
    let has_root_ca = sources.iter().any(|source| {
        source
            .pointer("/configMap/name")
            .and_then(|value| value.as_str())
            == Some("kube-root-ca.crt")
            && source
                .pointer("/configMap/items")
                .and_then(|value| value.as_array())
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("key").and_then(|value| value.as_str()) == Some("ca.crt")
                            && item.get("path").and_then(|value| value.as_str()) == Some("ca.crt")
                    })
                })
    });
    let has_namespace = sources.iter().any(|source| {
        source
            .pointer("/downwardAPI/items")
            .and_then(|value| value.as_array())
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("path").and_then(|value| value.as_str()) == Some("namespace")
                        && item
                            .pointer("/fieldRef/fieldPath")
                            .and_then(|value| value.as_str())
                            == Some("metadata.namespace")
                })
            })
    });

    has_token && has_root_ca && has_namespace
}

fn inject_serviceaccount_mounts(pod: &mut Value, volume_name: &str, containers_path: &str) {
    let Some(containers) = pod.pointer_mut(containers_path) else {
        return;
    };
    let Some(containers_arr) = containers.as_array_mut() else {
        return;
    };
    for container in containers_arr.iter_mut() {
        let Some(container_obj) = container.as_object_mut() else {
            continue;
        };
        let mounts = container_obj
            .entry("volumeMounts".to_string())
            .or_insert_with(|| json!([]));
        let Some(mounts_arr) = mounts.as_array_mut() else {
            continue;
        };
        if mounts_arr.iter().any(|mount| {
            mount.get("mountPath").and_then(|value| value.as_str())
                == Some(SERVICEACCOUNT_MOUNT_PATH)
        }) {
            continue;
        }
        mounts_arr.push(json!({
            "name": volume_name,
            "mountPath": SERVICEACCOUNT_MOUNT_PATH,
            "readOnly": true
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_default_kube_api_access_when_pod_has_explicit_token_projection() {
        let mut pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "oidc-discovery-validator", "namespace": "svcaccounts"},
            "spec": {
                "serviceAccountName": "default",
                "containers": [{
                    "name": "agnhost",
                    "image": "registry.k8s.io/e2e-test-images/agnhost:2.57",
                    "volumeMounts": [{
                        "name": "sa-token",
                        "mountPath": "/var/run/secrets/tokens",
                        "readOnly": true
                    }]
                }],
                "volumes": [{
                    "name": "sa-token",
                    "projected": {
                        "sources": [{
                            "serviceAccountToken": {
                                "path": "sa-token",
                                "audience": "oidc-discovery-test"
                            }
                        }]
                    }
                }]
            }
        });

        inject_serviceaccount_volume(&mut pod);

        let volumes = pod
            .pointer("/spec/volumes")
            .and_then(|value| value.as_array())
            .expect("pod must have volumes");
        let kube_api_access_volume = volumes
            .iter()
            .find(|volume| {
                volume
                    .get("name")
                    .and_then(|value| value.as_str())
                    .is_some_and(|name| name.starts_with("kube-api-access-"))
            })
            .expect("default kube-api-access projected volume must be injected");
        let kube_api_access_name = kube_api_access_volume
            .get("name")
            .and_then(|value| value.as_str())
            .expect("default volume must have a name");
        let sources = kube_api_access_volume
            .pointer("/projected/sources")
            .and_then(|value| value.as_array())
            .expect("default kube-api-access volume must have projected sources");
        assert!(
            sources
                .iter()
                .any(|source| source.get("serviceAccountToken").is_some()),
            "default kube-api-access volume must include the API token projection"
        );

        let mounts = pod
            .pointer("/spec/containers/0/volumeMounts")
            .and_then(|value| value.as_array())
            .expect("container must have volume mounts");
        assert!(
            mounts.iter().any(|mount| {
                mount.get("name").and_then(|value| value.as_str()) == Some("sa-token")
                    && mount.get("mountPath").and_then(|value| value.as_str())
                        == Some("/var/run/secrets/tokens")
            }),
            "explicit projected token mount must be preserved"
        );
        assert!(
            mounts.iter().any(|mount| {
                mount.get("name").and_then(|value| value.as_str()) == Some(kube_api_access_name)
                    && mount.get("mountPath").and_then(|value| value.as_str())
                        == Some(SERVICEACCOUNT_MOUNT_PATH)
                    && mount.get("readOnly").and_then(|value| value.as_bool()) == Some(true)
            }),
            "default service-account mount must be injected separately"
        );
    }
}
