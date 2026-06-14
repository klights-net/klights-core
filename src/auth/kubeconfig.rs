//! Kubernetes kubeconfig generation for klights.
//!
//! Generates a kubeconfig file that uses the cluster CA and admin certificates.
//! Writes the kubeconfig to a namespace-local etc directory.

use anyhow::Result;
use base64::Engine;

pub struct KubeconfigParams<'a> {
    pub ca_cert: &'a str,
    pub admin_cert: &'a str,
    pub admin_key: &'a str,
    pub tls_port: u16,
    pub context_name: &'a str,
    pub host_ip: Option<&'a str>,
    pub pod_subnet: &'a str,
}

/// Generate a kubeconfig YAML string.
///
/// klights never writes to ~/.kube. Use `KUBECONFIG` env var instead:
/// `export KUBECONFIG=$KLIGHTS_DATA_ROOT/etc/kubeconfig.yaml`
pub fn generate_kubeconfig(params: KubeconfigParams<'_>) -> Result<String> {
    let KubeconfigParams {
        ca_cert,
        admin_cert,
        admin_key,
        tls_port,
        context_name,
        host_ip,
        pod_subnet,
    } = params;
    let engine = base64::engine::general_purpose::STANDARD;

    let ca_b64 = engine.encode(ca_cert);
    let cert_b64 = engine.encode(admin_cert);
    let key_b64 = engine.encode(admin_key);

    let user_name = format!("{}-admin", context_name);

    let server_ip = host_ip
        .map(ToString::to_string)
        .unwrap_or_else(|| super::cert::derive_gateway_ip(pod_subnet));

    Ok(format!(
        r#"apiVersion: v1
kind: Config
clusters:
- cluster:
    certificate-authority-data: {ca_b64}
    server: https://{server_ip}:{tls_port}
  name: {context_name}
contexts:
- context:
    cluster: {context_name}
    user: {user_name}
  name: {context_name}
current-context: {context_name}
users:
- name: {user_name}
  user:
    client-certificate-data: {cert_b64}
    client-key-data: {key_b64}
"#
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_params() -> KubeconfigParams<'static> {
        KubeconfigParams {
            ca_cert: "ca",
            admin_cert: "cert",
            admin_key: "key",
            tls_port: 6443,
            context_name: "klights",
            host_ip: Some("192.168.8.22"),
            pod_subnet: "10.43.0.0/17",
        }
    }

    #[test]
    fn test_generate_kubeconfig_contains_required_fields() {
        let kubeconfig = generate_kubeconfig(KubeconfigParams {
            ca_cert: "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n",
            admin_cert: "-----BEGIN CERTIFICATE-----\nclient\n-----END CERTIFICATE-----\n",
            admin_key: "-----BEGIN RSA PRIVATE KEY-----\nkey\n-----END RSA PRIVATE KEY-----\n",
            ..default_params()
        })
        .unwrap();

        assert!(kubeconfig.contains("apiVersion: v1"));
        assert!(kubeconfig.contains("kind: Config"));
        assert!(kubeconfig.contains("name: klights"));
        assert!(kubeconfig.contains("user: klights-admin"));
        assert!(kubeconfig.contains("https://192.168.8.22:6443"));
        assert!(kubeconfig.contains("certificate-authority-data:"));
        assert!(kubeconfig.contains("client-certificate-data:"));
        assert!(kubeconfig.contains("client-key-data:"));
    }

    #[test]
    fn test_generate_kubeconfig_uses_correct_port() {
        let kubeconfig = generate_kubeconfig(KubeconfigParams {
            tls_port: 7679,
            host_ip: Some("192.168.1.100"),
            ..default_params()
        })
        .unwrap();
        assert!(kubeconfig.contains("https://192.168.1.100:7679"));
        assert!(!kubeconfig.contains("6443"));
    }

    #[test]
    fn test_generate_kubeconfig_uses_namespace_as_context_name() {
        let kubeconfig = generate_kubeconfig(KubeconfigParams {
            context_name: "klights-tester-1",
            pod_subnet: "10.50.0.0/17",
            ..default_params()
        })
        .unwrap();

        // Context name should match the namespace
        assert!(kubeconfig.contains("name: klights-tester-1"));
        assert!(kubeconfig.contains("current-context: klights-tester-1"));

        // Cluster and user names should also be namespace-aware
        assert!(kubeconfig.contains("cluster: klights-tester-1"));
        assert!(kubeconfig.contains("user: klights-tester-1-admin"));

        // Should NOT contain hardcoded "klights"
        assert!(!kubeconfig.contains("name: klights\n"));
    }

    #[test]
    fn test_generate_kubeconfig_default_namespace_uses_klights() {
        let kubeconfig = generate_kubeconfig(default_params()).unwrap();

        // Default namespace should still work
        assert!(kubeconfig.contains("name: klights"));
        assert!(kubeconfig.contains("current-context: klights"));
        assert!(kubeconfig.contains("cluster: klights"));
        assert!(kubeconfig.contains("user: klights-admin"));
    }

    #[test]
    fn test_kubeconfig_uses_host_ip() {
        // Kubeconfig must use host real IP, not pod gateway IP
        let kubeconfig = generate_kubeconfig(KubeconfigParams {
            tls_port: 7679,
            ..default_params()
        })
        .unwrap();

        // Must use host real IP
        assert!(
            kubeconfig.contains("https://192.168.8.22:7679"),
            "Kubeconfig must use host real IP"
        );

        // Must NOT use pod gateway IP
        assert!(
            !kubeconfig.contains("10.43.0.1"),
            "Kubeconfig must not use pod gateway IP"
        );
    }

    #[test]
    fn test_kubeconfig_fallback_to_gateway_when_no_host_ip() {
        // When host IP is unavailable, fall back to gateway IP (Option A)
        let kubeconfig = generate_kubeconfig(KubeconfigParams {
            tls_port: 7679,
            host_ip: None,
            ..default_params()
        })
        .unwrap();

        // Must fall back to gateway IP
        assert!(
            kubeconfig.contains("https://10.43.0.1:7679"),
            "Kubeconfig must fall back to gateway IP when host IP unavailable"
        );
    }

    #[test]
    fn test_kubeconfig_uses_provided_host_ip_not_derived() {
        // Kubeconfig uses the provided host IP, not derived from subnets
        let test_cases = vec![
            ("192.168.1.100", "https://192.168.1.100:7679"),
            ("10.0.0.50", "https://10.0.0.50:7679"),
            ("172.16.5.10", "https://172.16.5.10:7679"),
        ];

        for (host_ip, expected_url) in test_cases {
            let kubeconfig = generate_kubeconfig(KubeconfigParams {
                tls_port: 7679,
                host_ip: Some(host_ip),
                ..default_params()
            })
            .unwrap();
            assert!(
                kubeconfig.contains(expected_url),
                "Kubeconfig with host_ip {} should contain {}",
                host_ip,
                expected_url
            );
        }
    }
}
