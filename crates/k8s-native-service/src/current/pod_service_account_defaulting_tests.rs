use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use klights_cluster_core::Resource;
use serde_json::{Value, json};

use super::{
    AdmissionResourceStore,
    pod_service_account_defaulting::{
        apply_pod_service_account_defaulting, inject_service_account_projected_volume,
    },
};

struct FixedIdentity;

impl crate::ApiIdentityGenerator for FixedIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        format!("{prefix}abcde")
    }

    fn new_uid(&self) -> String {
        "uid-fixed".to_string()
    }
}

struct RecordingAdmissionStore {
    service_account: Option<Resource>,
    reads: AtomicUsize,
}

impl RecordingAdmissionStore {
    fn with_service_account(value: Value) -> Self {
        Self {
            service_account: Some(Resource::try_from_data(Arc::new(value)).expect("resource")),
            reads: AtomicUsize::new(0),
        }
    }

    fn missing() -> Self {
        Self {
            service_account: None,
            reads: AtomicUsize::new(0),
        }
    }

    fn read_count(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl AdmissionResourceStore for RecordingAdmissionStore {
    async fn get_admission_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>, klights_leader_api::ResourceQueryError> {
        assert_eq!(
            (api_version, kind, namespace, name),
            ("v1", "ServiceAccount", Some("team-a"), "builder")
        );
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.service_account.clone())
    }

    async fn list_admission_resources(
        &self,
        _api_version: &str,
        _kind: &str,
        _namespace: Option<&str>,
    ) -> Result<Vec<Resource>, klights_leader_api::ResourceQueryError> {
        panic!("service-account defaulting must not list resources")
    }
}

fn pod_spec(extra: Value) -> Value {
    let mut pod = json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "work", "namespace": "team-a"},
        "spec": {
            "serviceAccountName": "builder",
            "initContainers": [{"name": "init", "image": "busybox"}],
            "containers": [{"name": "app", "image": "busybox"}]
        }
    });
    if let Some(fields) = extra.as_object() {
        pod["spec"]
            .as_object_mut()
            .expect("spec")
            .extend(fields.clone());
    }
    pod
}

fn service_account(automount: Option<bool>) -> Value {
    let mut value = json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "builder", "namespace": "team-a"},
        "imagePullSecrets": [{"name": "registry-key"}]
    });
    if let Some(automount) = automount {
        value["automountServiceAccountToken"] = json!(automount);
    }
    value
}

#[test]
fn projected_volume_injection_is_idempotent_and_preserves_custom_projections() {
    let mut pod = pod_spec(json!({
        "volumes": [{
            "name": "custom-token",
            "projected": {"sources": [{"serviceAccountToken": {
                "path": "custom", "audience": "custom-audience"
            }}]}
        }],
        "containers": [{
            "name": "app", "image": "busybox",
            "volumeMounts": [{"name": "custom-token", "mountPath": "/var/run/custom"}]
        }]
    }));

    inject_service_account_projected_volume(&mut pod, &FixedIdentity);
    inject_service_account_projected_volume(&mut pod, &FixedIdentity);

    let volumes = pod["spec"]["volumes"].as_array().expect("volumes");
    assert_eq!(volumes.len(), 2);
    assert!(
        volumes
            .iter()
            .any(|volume| volume["name"] == "custom-token")
    );
    assert_eq!(
        volumes
            .iter()
            .filter(|volume| volume["name"] == "kube-api-access-abcde")
            .count(),
        1
    );
    for containers in ["initContainers", "containers"] {
        for container in pod["spec"][containers].as_array().expect("containers") {
            assert_eq!(
                container["volumeMounts"]
                    .as_array()
                    .expect("mounts")
                    .iter()
                    .filter(|mount| {
                        mount["mountPath"] == "/var/run/secrets/kubernetes.io/serviceaccount"
                    })
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn service_account_is_fetched_once_for_combined_inheritance_and_automount_policy() {
    let store = RecordingAdmissionStore::with_service_account(service_account(Some(false)));
    let mut pod = pod_spec(json!({}));

    apply_pod_service_account_defaulting(&store, &FixedIdentity, "team-a", &mut pod)
        .await
        .expect("defaulting");

    assert_eq!(store.read_count(), 1);
    assert_eq!(pod["spec"]["imagePullSecrets"][0]["name"], "registry-key");
    assert!(pod["spec"].get("volumes").is_none());
}

#[tokio::test]
async fn nonempty_pod_image_pull_secrets_still_fetches_unspecified_automount_policy() {
    let store = RecordingAdmissionStore::with_service_account(service_account(Some(false)));
    let mut pod = pod_spec(json!({"imagePullSecrets": [{"name": "pod-key"}]}));

    apply_pod_service_account_defaulting(&store, &FixedIdentity, "team-a", &mut pod)
        .await
        .expect("defaulting");

    assert_eq!(store.read_count(), 1);
    assert_eq!(pod["spec"]["imagePullSecrets"][0]["name"], "pod-key");
    assert!(pod["spec"].get("volumes").is_none());
}

#[tokio::test]
async fn explicit_pod_automount_precedence_and_fetch_elision_are_preserved() {
    let store = RecordingAdmissionStore::with_service_account(service_account(Some(false)));
    let mut pod = pod_spec(json!({
        "automountServiceAccountToken": true,
        "imagePullSecrets": [{"name": "pod-key"}]
    }));

    apply_pod_service_account_defaulting(&store, &FixedIdentity, "team-a", &mut pod)
        .await
        .expect("defaulting");

    assert_eq!(store.read_count(), 0);
    assert!(pod["spec"]["volumes"].as_array().is_some_and(|volumes| {
        volumes
            .iter()
            .any(|volume| volume["name"] == "kube-api-access-abcde")
    }));
}

#[tokio::test]
async fn image_pull_inheritance_does_not_override_explicit_pod_automount_false() {
    let store = RecordingAdmissionStore::with_service_account(service_account(Some(true)));
    let mut pod = pod_spec(json!({"automountServiceAccountToken": false}));

    apply_pod_service_account_defaulting(&store, &FixedIdentity, "team-a", &mut pod)
        .await
        .expect("defaulting");

    assert_eq!(store.read_count(), 1);
    assert_eq!(pod["spec"]["imagePullSecrets"][0]["name"], "registry-key");
    assert!(pod["spec"].get("volumes").is_none());
}

#[tokio::test]
async fn missing_service_account_defaults_automount_true_without_fabricating_pull_secrets() {
    let store = RecordingAdmissionStore::missing();
    let mut pod = pod_spec(json!({}));

    apply_pod_service_account_defaulting(&store, &FixedIdentity, "team-a", &mut pod)
        .await
        .expect("defaulting");

    assert_eq!(store.read_count(), 1);
    assert!(pod["spec"].get("imagePullSecrets").is_none());
    assert!(pod["spec"]["volumes"].as_array().is_some_and(|volumes| {
        volumes
            .iter()
            .any(|volume| volume["name"] == "kube-api-access-abcde")
    }));
}
