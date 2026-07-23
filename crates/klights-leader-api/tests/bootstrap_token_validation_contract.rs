use klights_leader_api::{
    BootstrapTokenScope, BootstrapTokenValidation, BootstrapTokenValidationError,
    BootstrapTokenValidationFuture, BootstrapTokenValidationRequest,
};

struct ObjectSafeValidator;

impl BootstrapTokenValidation for ObjectSafeValidator {
    fn validate_bootstrap_token(
        &self,
        _request: BootstrapTokenValidationRequest,
    ) -> BootstrapTokenValidationFuture<'_> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn request_preserves_exact_token_and_scope() {
    let request = BootstrapTokenValidationRequest::try_new(
        "abcdef.0123456789abcdef",
        BootstrapTokenScope::Worker,
    )
    .unwrap();
    assert_eq!(request.token(), "abcdef.0123456789abcdef");
    assert_eq!(request.scope(), BootstrapTokenScope::Worker);
    assert_eq!(
        request.into_parts(),
        (
            "abcdef.0123456789abcdef".to_string(),
            BootstrapTokenScope::Worker
        )
    );
}

#[test]
fn request_rejects_empty_token_without_normalizing_secret_bytes() {
    assert!(
        BootstrapTokenValidationRequest::try_new("", BootstrapTokenScope::Controlplane).is_err()
    );
    let request =
        BootstrapTokenValidationRequest::try_new(" token ", BootstrapTokenScope::Controlplane)
            .unwrap();
    assert_eq!(request.token(), " token ");
}

#[test]
fn validator_is_object_safe_and_error_preserves_rejection_reason() {
    let validator: &dyn BootstrapTokenValidation = &ObjectSafeValidator;
    let _ = validator;
    let error = BootstrapTokenValidationError::rejected("expired worker token");
    assert_eq!(error.to_string(), "expired worker token");
}
