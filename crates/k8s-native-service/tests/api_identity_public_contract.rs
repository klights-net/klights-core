use k8s_native_service::ApiIdentityGenerator;

struct ExternalIdentity;

impl ApiIdentityGenerator for ExternalIdentity {
    fn generate_name(&self, prefix: &str) -> String {
        format!("{prefix}external")
    }

    fn new_uid(&self) -> String {
        "external-uid".to_string()
    }
}

#[test]
fn external_consumer_can_use_the_exact_object_safe_identity_contract() {
    let identity: &dyn ApiIdentityGenerator = &ExternalIdentity;
    assert_eq!(identity.generate_name("pod-"), "pod-external");
    assert_eq!(identity.new_uid(), "external-uid");
}
