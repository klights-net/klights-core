fn assert_object_safe<T: ?Sized>() {}

#[test]
fn every_substitutable_auth_port_is_object_safe() {
    assert_object_safe::<dyn klights_auth::Authorizer>();
    assert_object_safe::<dyn klights_auth::Clock>();
    assert_object_safe::<dyn klights_auth::MonotonicClock>();
    assert_object_safe::<dyn klights_auth::CsrSigner>();
    assert_object_safe::<dyn klights_leader_api::LeaderBootstrapTokenAuthentication>();
    assert_object_safe::<dyn klights_leader_api::LeaderBoundTokenSubjectLookup>();
    assert_object_safe::<dyn klights_leader_api::LeaderServiceAccountSigningKeyState>();
    assert_object_safe::<dyn klights_auth::NodePolicyStore>();
    assert_object_safe::<dyn klights_auth::OidcDiscovery>();
    assert_object_safe::<dyn klights_auth::OidcValidator>();
    assert_object_safe::<
        dyn klights_auth::projected_service_account_token::ProjectedTokenResourceReader,
    >();
    assert_object_safe::<dyn klights_auth::RbacPolicyStore>();
    assert_object_safe::<dyn klights_auth::RbacResourceReader>();
    assert_object_safe::<dyn klights_auth::WebhookAuthenticator>();
    assert_object_safe::<dyn klights_auth::WebhookTokenReviewer>();
}
