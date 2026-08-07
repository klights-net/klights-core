//! Pod ServiceAccount create-time defaulting owned by the native API service.

use serde_json::{Value, json};

use super::{AdmissionResourceStore, defaulting::apply_pod_service_account_defaults};
use crate::{ApiIdentityGenerator, AppError};

const SERVICE_ACCOUNT_MOUNT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

pub(super) async fn apply_pod_service_account_defaulting(
    resources: &(impl AdmissionResourceStore + ?Sized),
    identity: &dyn ApiIdentityGenerator,
    namespace: &str,
    pod: &mut Value,
) -> Result<(), AppError> {
    let Some(spec) = pod.pointer_mut("/spec").and_then(Value::as_object_mut) else {
        return Ok(());
    };
    apply_pod_service_account_defaults(spec);

    let inherit_image_pull_secrets = spec
        .get("imagePullSecrets")
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty);
    let pod_automount = spec
        .get("automountServiceAccountToken")
        .and_then(Value::as_bool);
    let service_account_name = spec
        .get("serviceAccountName")
        .and_then(Value::as_str)
        .unwrap_or("default")
        .to_string();

    let service_account = if inherit_image_pull_secrets || pod_automount.is_none() {
        resources
            .get_admission_resource(
                "v1",
                "ServiceAccount",
                Some(namespace),
                &service_account_name,
            )
            .await?
    } else {
        None
    };

    if inherit_image_pull_secrets
        && let Some(image_pull_secrets) = service_account
            .as_ref()
            .and_then(|account| account.data.get("imagePullSecrets"))
            .and_then(Value::as_array)
            .filter(|secrets| !secrets.is_empty())
            .cloned()
        && let Some(spec) = pod.pointer_mut("/spec").and_then(Value::as_object_mut)
    {
        spec.insert(
            "imagePullSecrets".to_string(),
            Value::Array(image_pull_secrets),
        );
    }

    let automount = pod_automount
        .or_else(|| {
            service_account.as_ref().and_then(|account| {
                account
                    .data
                    .get("automountServiceAccountToken")
                    .and_then(Value::as_bool)
            })
        })
        .unwrap_or(true);
    if automount {
        inject_service_account_projected_volume(pod, identity);
    }
    Ok(())
}

pub(super) fn inject_service_account_projected_volume(
    pod: &mut Value,
    identity: &dyn ApiIdentityGenerator,
) {
    let existing_volume_name = default_projected_volume_name(pod);
    let volume_name = existing_volume_name
        .clone()
        .unwrap_or_else(|| identity.generate_name("kube-api-access-"));

    if existing_volume_name.is_none()
        && let Some(spec) = pod.pointer_mut("/spec").and_then(Value::as_object_mut)
    {
        let volumes = spec
            .entry("volumes".to_string())
            .or_insert_with(|| json!([]));
        if let Some(volumes) = volumes.as_array_mut() {
            volumes.push(json!({
                "name": volume_name.clone(),
                "projected": {
                    "defaultMode": 420,
                    "sources": [
                        {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                        {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                        {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}}
                    ]
                }
            }));
        }
    }

    inject_mounts(pod, &volume_name, "/spec/initContainers");
    inject_mounts(pod, &volume_name, "/spec/containers");
}

fn default_projected_volume_name(pod: &Value) -> Option<String> {
    pod.pointer("/spec/volumes")
        .and_then(Value::as_array)
        .and_then(|volumes| {
            volumes.iter().find_map(|volume| {
                let name = volume.get("name").and_then(Value::as_str)?;
                (name.starts_with("kube-api-access-") && has_default_projected_sources(volume))
                    .then(|| name.to_string())
            })
        })
}

fn has_default_projected_sources(volume: &Value) -> bool {
    let Some(sources) = volume
        .pointer("/projected/sources")
        .and_then(Value::as_array)
    else {
        return false;
    };
    let token = sources.iter().any(|source| {
        source
            .pointer("/serviceAccountToken/path")
            .and_then(Value::as_str)
            == Some("token")
    });
    let root_ca = sources.iter().any(|source| {
        source.pointer("/configMap/name").and_then(Value::as_str) == Some("kube-root-ca.crt")
            && source
                .pointer("/configMap/items")
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    items.iter().any(|item| {
                        item.get("key").and_then(Value::as_str) == Some("ca.crt")
                            && item.get("path").and_then(Value::as_str) == Some("ca.crt")
                    })
                })
    });
    let namespace = sources.iter().any(|source| {
        source
            .pointer("/downwardAPI/items")
            .and_then(Value::as_array)
            .is_some_and(|items| {
                items.iter().any(|item| {
                    item.get("path").and_then(Value::as_str) == Some("namespace")
                        && item.pointer("/fieldRef/fieldPath").and_then(Value::as_str)
                            == Some("metadata.namespace")
                })
            })
    });
    token && root_ca && namespace
}

fn inject_mounts(pod: &mut Value, volume_name: &str, containers_path: &str) {
    let Some(containers) = pod
        .pointer_mut(containers_path)
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for container in containers {
        let Some(container) = container.as_object_mut() else {
            continue;
        };
        let mounts = container
            .entry("volumeMounts".to_string())
            .or_insert_with(|| json!([]));
        let Some(mounts) = mounts.as_array_mut() else {
            continue;
        };
        if mounts.iter().any(|mount| {
            mount.get("mountPath").and_then(Value::as_str) == Some(SERVICE_ACCOUNT_MOUNT_PATH)
        }) {
            continue;
        }
        mounts.push(json!({
            "name": volume_name,
            "mountPath": SERVICE_ACCOUNT_MOUNT_PATH,
            "readOnly": true
        }));
    }
}
