//! Kubelet client certificate and CSR generation.
//!
//! Produces CSR PEM bytes and private keys for worker node TLS bootstrap.
//! Uses rcgen for key generation and CSR serialization.

use anyhow::{Context, Result};

/// A generated CSR and private key for a kubelet bootstrap request.
#[derive(Clone, Debug)]
pub struct KubeletClientCsr {
    pub csr_pem: Vec<u8>,
    pub private_key_pem: String,
    pub node_name: String,
}

/// Generate a kubelet client CSR for `system:node:<nodeName>`.
///
/// The CSR includes:
/// - CN = `system:node:<nodeName>`
/// - O = `system:nodes`
/// - client auth extended key usage
pub fn generate_kubelet_client_csr(node_name: &str) -> Result<KubeletClientCsr> {
    use rcgen::{CertificateParams, DnType, KeyPair, KeyUsagePurpose};

    let mut params = CertificateParams::default();
    params.distinguished_name = rcgen::DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, format!("system:node:{node_name}"));
    params
        .distinguished_name
        .push(DnType::OrganizationName, "system:nodes".to_string());
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];

    let key_pair =
        KeyPair::generate().context("failed to generate key pair for kubelet client CSR")?;
    let csr = params
        .serialize_request(&key_pair)
        .context("failed to serialize kubelet client CSR")?;
    let csr_pem = csr
        .pem()
        .context("failed to PEM-encode kubelet client CSR")?;
    let private_key_pem = key_pair.serialize_pem();

    Ok(KubeletClientCsr {
        csr_pem: csr_pem.into_bytes(),
        private_key_pem,
        node_name: node_name.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_csr_has_correct_common_name() {
        let csr = generate_kubelet_client_csr("tokyo").unwrap();
        assert_eq!(csr.node_name, "tokyo");

        // Verify the CSR PEM starts with the expected header
        let pem_str = String::from_utf8_lossy(&csr.csr_pem);
        assert!(pem_str.contains("-----BEGIN CERTIFICATE REQUEST-----"));
        assert!(pem_str.contains("-----END CERTIFICATE REQUEST-----"));

        // Verify private key is present
        assert!(
            csr.private_key_pem.contains("-----BEGIN PRIVATE KEY-----")
                || csr
                    .private_key_pem
                    .contains("-----BEGIN EC PRIVATE KEY-----")
        );
    }

    #[test]
    fn test_generate_csr_can_be_parsed_by_x509_parser() {
        let csr = generate_kubelet_client_csr("tokyo").unwrap();

        // Verify the CSR can be parsed and has the expected subject
        use x509_parser::pem::Pem;
        use x509_parser::prelude::*;

        let pem = Pem::read(std::io::Cursor::new(&csr.csr_pem)).unwrap().0;
        let (_, parsed_csr) = X509CertificationRequest::from_der(&pem.contents).unwrap();
        let subject = parsed_csr.certification_request_info.subject;

        let mut cn = String::new();
        let mut orgs = Vec::new();
        for attr in subject.iter_attributes() {
            if attr.attr_type() == &x509_parser::oid_registry::OID_X509_COMMON_NAME {
                if let Ok(s) = attr.as_str() {
                    cn = s.to_string();
                }
            } else if attr.attr_type() == &x509_parser::oid_registry::OID_X509_ORGANIZATION_NAME
                && let Ok(s) = attr.as_str()
            {
                orgs.push(s.to_string());
            }
        }

        assert_eq!(cn, "system:node:tokyo");
        assert!(orgs.contains(&"system:nodes".to_string()));
    }

    #[test]
    fn test_generate_csr_with_different_node_names() {
        let csr1 = generate_kubelet_client_csr("tokyo").unwrap();
        let csr2 = generate_kubelet_client_csr("osaka").unwrap();

        // Each CSR should have different content since keys differ
        assert_ne!(csr1.csr_pem, csr2.csr_pem);
        assert_eq!(csr1.node_name, "tokyo");
        assert_eq!(csr2.node_name, "osaka");
    }

    #[test]
    fn test_generate_csr_private_key_is_valid_pem() {
        let csr = generate_kubelet_client_csr("tokyo").unwrap();
        // Private key should be non-empty PEM
        assert!(!csr.private_key_pem.is_empty());
        assert!(csr.private_key_pem.starts_with("-----BEGIN"));
    }
}
