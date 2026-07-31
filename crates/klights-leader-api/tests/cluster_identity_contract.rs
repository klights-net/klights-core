use klights_leader_api::{
    BootstrapTokenIdentity, ClusterIdentityError, LeaderBootstrapTokenAuthentication,
    LeaderBoundTokenSubjectLookup, LeaderServiceAccountSigningKeyState,
    ServiceAccountSigningKeyPem,
};

fn assert_object_safe<T: ?Sized>() {}

#[test]
fn cluster_identity_ports_remain_object_safe() {
    assert_object_safe::<dyn LeaderBootstrapTokenAuthentication>();
    assert_object_safe::<dyn LeaderBoundTokenSubjectLookup>();
    assert_object_safe::<dyn LeaderServiceAccountSigningKeyState>();
}

#[test]
fn bootstrap_identity_rejects_an_empty_username() {
    let error = BootstrapTokenIdentity::try_new("", vec!["system:bootstrappers".to_string()])
        .expect_err("empty usernames must not cross the leader API boundary");

    assert_eq!(
        error,
        ClusterIdentityError::internal_failure("bootstrap identity token ID must not be empty")
    );
}

#[test]
fn bootstrap_identity_preserves_the_validated_payload() {
    let identity = BootstrapTokenIdentity::try_new(
        "system:bootstrap:abcdef",
        vec!["system:bootstrappers".to_string()],
    )
    .expect("valid bootstrap identity");

    assert_eq!(identity.token_id(), "system:bootstrap:abcdef");
    assert_eq!(identity.extra_groups(), &["system:bootstrappers"]);
}

#[test]
fn signing_key_state_rejects_empty_pem() {
    let error = ServiceAccountSigningKeyPem::try_new(" \n")
        .expect_err("empty signing state must not cross the leader API boundary");

    assert_eq!(
        error,
        ClusterIdentityError::internal_failure("ServiceAccount signing key PEM must not be empty")
    );
}

#[test]
fn signing_key_state_preserves_non_empty_pem() {
    let pem = ServiceAccountSigningKeyPem::try_new("-----BEGIN PRIVATE KEY-----\nopaque")
        .expect("non-empty persistence payload");

    assert_eq!(pem.as_str(), "-----BEGIN PRIVATE KEY-----\nopaque");
    assert_eq!(
        pem.into_string(),
        "-----BEGIN PRIVATE KEY-----\nopaque".to_string()
    );
}
