use super::*;

fn bootstrap_identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::bootstrap("abcdef", &[])
}

fn sa_identity() -> AuthenticatedIdentity {
    AuthenticatedIdentity::service_account(
        "system:serviceaccount:default:my-sa".to_string(),
        vec![
            "system:serviceaccounts".to_string(),
            "system:serviceaccounts:default".to_string(),
        ],
        Some("uid-123".to_string()),
    )
}

#[test]
fn stamp_csr_identity_overwrites_client_supplied_fields() {
    let mut body = serde_json::json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": { "name": "my-csr" },
        "spec": {
            "request": "dGhpcyBpcyBmYWtl",
            "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
            "username": "forged-user",
            "groups": ["forged-group"],
            "uid": "forged-uid"
        }
    });

    let identity = bootstrap_identity();
    stamp_csr_identity(&mut body, &identity);

    let spec = body.get("spec").unwrap();
    assert_eq!(spec["username"], "system:bootstrap:abcdef");
    assert!(
        spec["groups"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("system:bootstrappers"))
    );
    assert!(
        !spec["groups"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("forged-group"))
    );
    assert_ne!(spec["uid"], "forged-uid");
}

#[test]
fn stamp_csr_identity_creates_spec_when_absent() {
    let mut body = serde_json::json!({
        "apiVersion": "certificates.k8s.io/v1",
        "kind": "CertificateSigningRequest",
        "metadata": { "name": "my-csr" }
    });

    let identity = bootstrap_identity();
    stamp_csr_identity(&mut body, &identity);

    let spec = body.get("spec").unwrap();
    assert_eq!(spec["username"], "system:bootstrap:abcdef");
}

#[test]
fn stamp_csr_identity_uses_uid_from_identity_when_present() {
    let mut body = serde_json::json!({
        "metadata": { "name": "csr" },
        "spec": { "signerName": "kubernetes.io/kube-apiserver-client-kubelet" }
    });

    let identity = sa_identity();
    stamp_csr_identity(&mut body, &identity);

    let spec = body.get("spec").unwrap();
    assert_eq!(spec["uid"], "uid-123");
}

#[test]
fn stamp_csr_identity_falls_back_to_username_for_uid() {
    let mut body = serde_json::json!({
        "metadata": { "name": "csr" },
        "spec": {}
    });

    let identity = bootstrap_identity();
    stamp_csr_identity(&mut body, &identity);

    let spec = body.get("spec").unwrap();
    assert_eq!(spec["uid"], "system:bootstrap:abcdef");
}
