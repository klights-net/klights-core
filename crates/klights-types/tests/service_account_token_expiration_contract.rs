use klights_types::{
    DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS, MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
    MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS,
    normalize_service_account_token_expiration_seconds,
};

#[test]
fn service_account_token_expiration_defaults_and_clamps() {
    assert_eq!(
        normalize_service_account_token_expiration_seconds(None),
        DEFAULT_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
    );
    assert_eq!(
        normalize_service_account_token_expiration_seconds(Some(1)),
        MIN_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
    );
    assert_eq!(
        normalize_service_account_token_expiration_seconds(Some(i64::MAX)),
        MAX_SERVICE_ACCOUNT_TOKEN_EXPIRATION_SECONDS
    );
    assert_eq!(
        normalize_service_account_token_expiration_seconds(Some(7_200)),
        7_200
    );
}
