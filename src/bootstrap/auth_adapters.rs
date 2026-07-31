//! Root-owned adapters that connect auth policy ports to concrete stores.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::controllers::csr_signer::{
    CsrIssuanceError, CsrIssuanceOutcome, CsrIssuanceRequest, CsrIssuer, IssuedCsr,
};
use crate::datastore::backend::DatastoreHandle;
use crate::datastore::types::ListPageRequest;
use crate::kubelet::pod_repository::PodReader;
use klights_auth::node_policy_store::NodePolicyStore;
use klights_auth::rbac_policy_store::RbacResourceReader;
use klights_leader_rpc::server::{
    ControlplaneCredentialError, ControlplaneCredentialIssuer, ReplicationPeerAuthenticationError,
    ReplicationPeerAuthenticator, ReplicationPeerIdentity,
};

/// Root-owned bridge from bootstrap token Secrets to the API authentication
/// policy port. The HTTP/API owner receives only the auth-domain capability.
pub(crate) struct DatastoreBootstrapTokenAuthenticator {
    db: DatastoreHandle,
}

impl DatastoreBootstrapTokenAuthenticator {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

impl klights_leader_api::LeaderBootstrapTokenAuthentication
    for DatastoreBootstrapTokenAuthenticator
{
    fn authenticate_bootstrap_token<'a>(
        &'a self,
        token: &'a str,
    ) -> klights_leader_api::ClusterIdentityFuture<'a, klights_leader_api::BootstrapTokenIdentity>
    {
        Box::pin(async move {
            crate::bootstrap::bootstrap_token::validate_bootstrap_token(self.db.as_ref(), token)
                .await
                .map_err(|error| match error {
                    crate::bootstrap::bootstrap_token::BootstrapTokenAuthenticationError::Rejected {
                        message,
                    } => klights_leader_api::ClusterIdentityError::rejected(message),
                    crate::bootstrap::bootstrap_token::BootstrapTokenAuthenticationError::DependencyFailure {
                        message,
                    } => klights_leader_api::ClusterIdentityError::dependency_failure(message),
                    crate::bootstrap::bootstrap_token::BootstrapTokenAuthenticationError::InternalFailure {
                        message,
                    } => klights_leader_api::ClusterIdentityError::internal_failure(message),
                })
                .and_then(|identity| {
                    klights_leader_api::BootstrapTokenIdentity::try_new(
                        identity.token_id,
                        identity.extra_groups,
                    )
                })
        })
    }
}

const RBAC_API_VERSION: &str = "rbac.authorization.k8s.io/v1";

/// Root adapter joining auth-owned CSR policy, key signing, and wall-clock
/// capabilities behind the controller-owned issuance port.
pub(crate) struct AuthCsrIssuer {
    policy: klights_auth::csr_signer::KubeletCredentialPolicy,
}

impl AuthCsrIssuer {
    pub(crate) fn new(
        signer: Arc<dyn klights_auth::csr_signer::CsrSigner>,
        clock: Arc<dyn klights_auth::clock::Clock>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            policy: klights_auth::csr_signer::KubeletCredentialPolicy::new(
                signer, clock, supervisor,
            ),
        }
    }
}

#[async_trait]
impl CsrIssuer for AuthCsrIssuer {
    async fn issue(
        &self,
        request: CsrIssuanceRequest,
    ) -> Result<CsrIssuanceOutcome, CsrIssuanceError> {
        let outcome = self
            .policy
            .issue(klights_auth::KubeletCertificateRequest {
                signer_name: request.signer_name,
                csr_pem: request.csr_pem,
                usages: request.usages,
                username: request.username,
                groups: request.groups,
                expiration_seconds: request.expiration_seconds,
            })
            .await
            .map_err(|error| match error {
                klights_auth::CredentialOperationError::DependencyFailure { message } => {
                    CsrIssuanceError::DependencyFailure { message }
                }
                klights_auth::CredentialOperationError::Rejected { message }
                | klights_auth::CredentialOperationError::InternalFailure { message } => {
                    CsrIssuanceError::InternalFailure { message }
                }
            })?;
        Ok(match outcome {
            klights_auth::KubeletCertificateOutcome::Issued {
                node_name,
                certificate_pem,
                issued_at_unix_seconds,
            } => CsrIssuanceOutcome::Issued(IssuedCsr {
                node_name,
                certificate_pem,
                issued_at: time::OffsetDateTime::from_unix_timestamp(issued_at_unix_seconds)
                    .map_err(|error| CsrIssuanceError::InternalFailure {
                        message: format!("invalid auth issuance timestamp: {error}"),
                    })?,
            }),
            klights_auth::KubeletCertificateOutcome::Rejected { reason } => {
                CsrIssuanceOutcome::Rejected { reason }
            }
        })
    }
}

pub(crate) struct AuthReplicationPeerAuthenticator {
    policy: klights_auth::csr_signer::PeerCertificatePolicy,
}

impl AuthReplicationPeerAuthenticator {
    pub(crate) fn new(supervisor: Arc<klights_supervisor::TaskSupervisor>) -> Self {
        Self {
            policy: klights_auth::csr_signer::PeerCertificatePolicy::new(supervisor),
        }
    }
}

#[async_trait]
impl ReplicationPeerAuthenticator for AuthReplicationPeerAuthenticator {
    async fn authenticate(
        &self,
        certificate: &klights_types::TlsClientCertificate,
    ) -> Result<ReplicationPeerIdentity, ReplicationPeerAuthenticationError> {
        let user = self
            .policy
            .authenticate(certificate.clone())
            .await
            .map_err(|error| match error {
                klights_auth::AuthenticationError::Unauthenticated { message } => {
                    ReplicationPeerAuthenticationError::Rejected { message }
                }
                klights_auth::AuthenticationError::DependencyFailure { message } => {
                    ReplicationPeerAuthenticationError::DependencyFailure { message }
                }
                klights_auth::AuthenticationError::InternalFailure { message } => {
                    ReplicationPeerAuthenticationError::InternalFailure { message }
                }
            })?;
        Ok(ReplicationPeerIdentity {
            username: user.username,
            groups: user.groups,
        })
    }
}

pub(crate) struct AuthControlplaneCredentialIssuer {
    policy: klights_auth::csr_signer::ControlplaneCredentialPolicy,
}

impl AuthControlplaneCredentialIssuer {
    pub(crate) fn new(
        clock: Arc<dyn klights_auth::clock::Clock>,
        supervisor: Arc<klights_supervisor::TaskSupervisor>,
    ) -> Self {
        Self {
            policy: klights_auth::csr_signer::ControlplaneCredentialPolicy::new(clock, supervisor),
        }
    }
}

#[async_trait]
impl ControlplaneCredentialIssuer for AuthControlplaneCredentialIssuer {
    async fn sign_server_csr(
        &self,
        ca_cert_pem: &str,
        ca_key_pem: &str,
        csr_pem: Vec<u8>,
    ) -> Result<String, ControlplaneCredentialError> {
        self.policy
            .sign_server_csr(ca_cert_pem.to_string(), ca_key_pem.to_string(), csr_pem)
            .await
            .map_err(map_credential_operation_error)
    }

    async fn encrypt_key_material(
        &self,
        join_token: &str,
        plaintext: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), ControlplaneCredentialError> {
        self.policy
            .encrypt_key_material(join_token.to_string(), plaintext.to_vec())
            .await
            .map_err(map_credential_operation_error)
    }
}

fn map_credential_operation_error(
    error: klights_auth::CredentialOperationError,
) -> ControlplaneCredentialError {
    match error {
        klights_auth::CredentialOperationError::Rejected { message } => {
            ControlplaneCredentialError::Rejected { message }
        }
        klights_auth::CredentialOperationError::DependencyFailure { message } => {
            ControlplaneCredentialError::DependencyFailure { message }
        }
        klights_auth::CredentialOperationError::InternalFailure { message } => {
            ControlplaneCredentialError::InternalFailure { message }
        }
    }
}

pub(crate) struct DatastoreRbacResourceReader {
    db: DatastoreHandle,
}

impl DatastoreRbacResourceReader {
    pub(crate) fn new(db: DatastoreHandle) -> Self {
        Self { db }
    }
}

#[async_trait]
impl RbacResourceReader for DatastoreRbacResourceReader {
    async fn list_cluster_rbac_resources(
        &self,
        kind: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        let list = self
            .db
            .list_resources_page(
                RBAC_API_VERSION,
                kind,
                None,
                None,
                None,
                ListPageRequest::unbounded(),
            )
            .await
            .map_err(|error| format!("failed to list cluster RBAC resources: {error}"))?;
        Ok(list
            .items
            .into_iter()
            .map(|resource| resource.data.as_ref().clone())
            .collect())
    }

    async fn list_namespaced_rbac_resources(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        let list = self
            .db
            .list_resources_page(
                RBAC_API_VERSION,
                kind,
                Some(namespace),
                None,
                None,
                ListPageRequest::unbounded(),
            )
            .await
            .map_err(|error| format!("failed to list namespaced RBAC resources: {error}"))?;
        Ok(list
            .items
            .into_iter()
            .map(|resource| resource.data.as_ref().clone())
            .collect())
    }
}

/// Root adapter from the concrete Pod repository read surface to auth's
/// transport-neutral node relationship policy port.
pub(crate) struct PodRepositoryNodePolicyStore {
    pods: Arc<dyn PodReader>,
}

impl PodRepositoryNodePolicyStore {
    pub(crate) fn new(pods: Arc<dyn PodReader>) -> Self {
        Self { pods }
    }
}

#[async_trait]
impl NodePolicyStore for PodRepositoryNodePolicyStore {
    async fn get_pod_node(&self, namespace: &str, name: &str) -> Option<String> {
        let pod = self.pods.get_pod(namespace, name).await.ok().flatten()?;
        pod.data
            .pointer("/spec/nodeName")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
    }

    async fn list_pods_on_node(&self, node_name: &str) -> Vec<(String, String)> {
        let Ok(pods) = self.pods.list_pods(None, None, None, None, None).await else {
            return Vec::new();
        };
        pods.items
            .iter()
            .filter(|pod| {
                pod.data
                    .pointer("/spec/nodeName")
                    .and_then(serde_json::Value::as_str)
                    == Some(node_name)
            })
            .map(|pod| (pod.namespace.clone().unwrap_or_default(), pod.name.clone()))
            .collect()
    }

    async fn get_pod_referenced_objects(
        &self,
        namespace: &str,
        pod_name: &str,
        resource: &str,
    ) -> Vec<String> {
        let Ok(Some(pod)) = self.pods.get_pod(namespace, pod_name).await else {
            return Vec::new();
        };
        extract_referenced_objects(&pod.data, resource)
    }
}

fn extract_referenced_objects(pod: &serde_json::Value, resource: &str) -> Vec<String> {
    let mut names = HashSet::new();
    match resource {
        "secrets" => {
            if let Some(volumes) = pod
                .pointer("/spec/volumes")
                .and_then(serde_json::Value::as_array)
            {
                for volume in volumes {
                    if let Some(name) = volume
                        .get("secret")
                        .and_then(|secret| secret.get("secretName"))
                        .and_then(serde_json::Value::as_str)
                    {
                        names.insert(name.to_string());
                    }
                }
            }
            extract_env_from_refs(pod, "secretRef", &mut names);
            if let Some(pull_secrets) = pod
                .pointer("/spec/imagePullSecrets")
                .and_then(serde_json::Value::as_array)
            {
                for pull_secret in pull_secrets {
                    if let Some(name) = pull_secret.get("name").and_then(serde_json::Value::as_str)
                    {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        "configmaps" => {
            if let Some(volumes) = pod
                .pointer("/spec/volumes")
                .and_then(serde_json::Value::as_array)
            {
                for volume in volumes {
                    if let Some(name) = volume
                        .get("configMap")
                        .and_then(|config_map| config_map.get("name"))
                        .and_then(serde_json::Value::as_str)
                    {
                        names.insert(name.to_string());
                    }
                }
            }
            extract_env_from_refs(pod, "configMapRef", &mut names);
        }
        "persistentvolumeclaims" => {
            if let Some(volumes) = pod
                .pointer("/spec/volumes")
                .and_then(serde_json::Value::as_array)
            {
                for volume in volumes {
                    if let Some(name) = volume
                        .get("persistentVolumeClaim")
                        .and_then(|claim| claim.get("claimName"))
                        .and_then(serde_json::Value::as_str)
                    {
                        names.insert(name.to_string());
                    }
                }
            }
        }
        "serviceaccounts" => {
            if let Some(name) = pod
                .pointer("/spec/serviceAccountName")
                .and_then(serde_json::Value::as_str)
            {
                names.insert(name.to_string());
            }
        }
        _ => {}
    }
    names.into_iter().collect()
}

fn extract_env_from_refs(pod: &serde_json::Value, ref_key: &str, names: &mut HashSet<String>) {
    for container_path in ["/spec/containers", "/spec/initContainers"] {
        if let Some(containers) = pod
            .pointer(container_path)
            .and_then(serde_json::Value::as_array)
        {
            for container in containers {
                if let Some(env_from) = container
                    .get("envFrom")
                    .and_then(serde_json::Value::as_array)
                {
                    for source in env_from {
                        if let Some(name) = source
                            .get(ref_key)
                            .and_then(|reference| reference.get("name"))
                            .and_then(serde_json::Value::as_str)
                        {
                            names.insert(name.to_string());
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{PodRepositoryNodePolicyStore, extract_referenced_objects};
    use crate::datastore::Resource;
    use crate::kubelet::pod_repository::PodReader;
    use crate::kubelet::pod_repository::PodResourceList as ResourceList;
    use klights_auth::node_policy_store::NodePolicyStore;

    struct FakePodReader {
        pods: Vec<Resource>,
    }

    #[async_trait]
    impl PodReader for FakePodReader {
        async fn get_pod(&self, namespace: &str, name: &str) -> anyhow::Result<Option<Resource>> {
            Ok(self
                .pods
                .iter()
                .find(|pod| pod.namespace.as_deref() == Some(namespace) && pod.name == name)
                .cloned())
        }

        async fn get_pod_for_uid(
            &self,
            namespace: &str,
            name: &str,
            uid: &str,
        ) -> anyhow::Result<Option<Resource>> {
            Ok(self
                .pods
                .iter()
                .find(|pod| {
                    pod.namespace.as_deref() == Some(namespace)
                        && pod.name == name
                        && pod.uid == uid
                })
                .cloned())
        }

        async fn list_pods(
            &self,
            _namespace: Option<&str>,
            _label_selector: Option<&str>,
            _field_selector: Option<&str>,
            _limit: Option<i64>,
            _continue_token: Option<&str>,
        ) -> anyhow::Result<ResourceList> {
            Ok(ResourceList {
                items: self.pods.clone(),
                resource_version: 1,
                continue_token: None,
                remaining_item_count: None,
            })
        }

        async fn list_pods_by_owner_uid(
            &self,
            _namespace: &str,
            _owner_uid: &str,
        ) -> anyhow::Result<Vec<Resource>> {
            Ok(Vec::new())
        }
    }

    fn pod(name: &str, namespace: &str, node: &str) -> Resource {
        Resource::from_data_lossy(Arc::new(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": format!("uid-{name}")
            },
            "spec": {
                "nodeName": node,
                "serviceAccountName": "default-sa",
                "volumes": [{"secret": {"secretName": "volume-secret"}}],
                "containers": [{"envFrom": [{"configMapRef": {"name": "env-config"}}]}]
            }
        })))
    }

    #[tokio::test]
    async fn pod_repository_adapter_projects_only_node_policy_values() {
        let store = PodRepositoryNodePolicyStore::new(Arc::new(FakePodReader {
            pods: vec![
                pod("pod-a", "default", "tokyo"),
                pod("coredns", "kube-system", "tokyo"),
                pod("pod-b", "default", "osaka"),
            ],
        }));

        assert_eq!(
            store.get_pod_node("default", "pod-a").await.as_deref(),
            Some("tokyo")
        );
        let mut tokyo = store.list_pods_on_node("tokyo").await;
        tokyo.sort();
        assert_eq!(
            tokyo,
            vec![
                ("default".to_string(), "pod-a".to_string()),
                ("kube-system".to_string(), "coredns".to_string()),
            ]
        );
        assert_eq!(
            store
                .get_pod_referenced_objects("default", "pod-a", "secrets")
                .await,
            vec!["volume-secret".to_string()]
        );
    }

    #[test]
    fn pod_reference_extraction_covers_node_authorizer_resource_families() {
        let pod = serde_json::json!({
            "spec": {
                "serviceAccountName": "default-sa",
                "containers": [{
                    "envFrom": [
                        {"configMapRef": {"name": "env-config"}},
                        {"secretRef": {"name": "env-secret"}}
                    ]
                }],
                "initContainers": [{
                    "envFrom": [{"secretRef": {"name": "init-secret"}}]
                }],
                "volumes": [
                    {"configMap": {"name": "volume-config"}},
                    {"secret": {"secretName": "volume-secret"}},
                    {"persistentVolumeClaim": {"claimName": "data"}}
                ],
                "imagePullSecrets": [{"name": "registry-secret"}]
            }
        });
        let cases = [
            (
                "secrets",
                vec![
                    "env-secret",
                    "init-secret",
                    "registry-secret",
                    "volume-secret",
                ],
            ),
            ("configmaps", vec!["env-config", "volume-config"]),
            ("persistentvolumeclaims", vec!["data"]),
            ("serviceaccounts", vec!["default-sa"]),
            ("unknown", vec![]),
        ];

        for (resource, expected) in cases {
            let mut actual = extract_referenced_objects(&pod, resource);
            actual.sort();
            assert_eq!(actual, expected, "resource={resource}");
        }
    }
}
