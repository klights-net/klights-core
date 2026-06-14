//! Webhook token auth tests.
//!
//! Tests use a mock `WebhookTokenReviewer` to verify all code paths
//! without network access.
//!
//! Wire format tests (TokenReview JSON round-trip) are in
//! `src/auth/webhook_auth.rs` `#[cfg(test)] mod tests`.

use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::webhook_auth::*;
use std::sync::Arc;
use std::time::Duration;

// ─── Mock implementation ───────────────────────────────────────────────────

struct MockWebhookReviewer {
    result: Result<Option<TokenReviewStatus>, String>,
    call_count: std::sync::Mutex<usize>,
}

impl MockWebhookReviewer {
    fn new(result: Result<Option<TokenReviewStatus>, String>) -> Self {
        Self {
            result,
            call_count: std::sync::Mutex::new(0),
        }
    }

    fn call_count(&self) -> usize {
        *self.call_count.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl WebhookTokenReviewer for MockWebhookReviewer {
    async fn review_token(
        &self,
        _token: &str,
        _audiences: &[String],
    ) -> Result<Option<TokenReviewStatus>, String> {
        *self.call_count.lock().unwrap() += 1;
        self.result.clone()
    }
}

fn reviewer_arc(result: Result<Option<TokenReviewStatus>, String>) -> Arc<MockWebhookReviewer> {
    Arc::new(MockWebhookReviewer::new(result))
}

fn make_cached_auth(
    reviewer: Arc<MockWebhookReviewer>,
    authorized_ttl: Duration,
    unauthorized_ttl: Duration,
) -> WebhookAuth {
    WebhookAuth::new(
        reviewer as Arc<dyn WebhookTokenReviewer>,
        authorized_ttl,
        unauthorized_ttl,
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
    )
}

fn auth_status(user: TokenReviewUser) -> TokenReviewStatus {
    TokenReviewStatus {
        authenticated: true,
        user: Some(user),
        error: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
    }
}

fn unauth_status() -> TokenReviewStatus {
    TokenReviewStatus {
        authenticated: false,
        user: None,
        error: None,
        audiences: vec![],
    }
}

fn test_user(name: &str) -> TokenReviewUser {
    TokenReviewUser {
        username: name.to_string(),
        uid: Some(format!("uid-{name}")),
        groups: vec!["developers".to_string(), "viewers".to_string()],
        extra: vec![("org".to_string(), "engineering".to_string())],
    }
}

// ─── Cache tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cache_hit_no_second_webhook_call() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("alice")))));
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    let r1 = auth.authenticate("same-token").await;
    assert!(r1.is_some());
    assert!(r1.unwrap().is_ok());
    assert_eq!(reviewer.call_count(), 1);

    let r2 = auth.authenticate("same-token").await;
    assert!(r2.is_some());
    assert!(r2.unwrap().is_ok());
    assert_eq!(reviewer.call_count(), 1, "cache should prevent second call");
}

#[tokio::test]
async fn test_cache_miss_different_tokens_call_webhook() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("alice")))));
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    assert!(auth.authenticate("token-a").await.is_some());
    assert!(auth.authenticate("token-b").await.is_some());
    assert_eq!(
        reviewer.call_count(),
        2,
        "different tokens = different cache keys"
    );
}

#[tokio::test]
async fn test_cache_expired_rechecks_webhook() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("alice")))));
    let auth = WebhookAuth::new(
        reviewer.clone() as Arc<dyn WebhookTokenReviewer>,
        Duration::ZERO,
        Duration::ZERO,
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
    );

    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(reviewer.call_count(), 1);

    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(reviewer.call_count(), 2);
}

#[tokio::test]
async fn test_cache_unauthorized_cached() {
    let reviewer = reviewer_arc(Ok(Some(unauth_status())));
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    let r = auth.authenticate("bad-token").await;
    assert!(r.is_some());
    assert!(r.unwrap().is_err());
    assert_eq!(reviewer.call_count(), 1);

    let _ = auth.authenticate("bad-token").await;
    assert_eq!(reviewer.call_count(), 1, "unauthorized result also cached");
}

#[tokio::test]
async fn test_cache_authorized_and_unauthorized_different_ttls() {
    let reviewer = reviewer_arc(Ok(Some(unauth_status())));
    let auth = WebhookAuth::new(
        reviewer.clone() as Arc<dyn WebhookTokenReviewer>,
        Duration::from_secs(60),
        Duration::ZERO,
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
    );

    auth.authenticate("bad").await;
    assert_eq!(reviewer.call_count(), 1);
    auth.authenticate("bad").await;
    assert_eq!(
        reviewer.call_count(),
        2,
        "unauthorized TTL=0 means always re-check"
    );
}

#[tokio::test]
async fn test_cache_error_is_cached_with_unauthorized_ttl() {
    let reviewer = reviewer_arc(Err("timeout".to_string()));
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    let _ = auth.authenticate("token").await;
    let _ = auth.authenticate("token").await;
    assert_eq!(
        reviewer.call_count(),
        1,
        "errors also cached with unauthorized TTL"
    );
}

#[tokio::test]
async fn test_cache_capacity_evicts_oldest_entry() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("alice")))));
    let auth = WebhookAuth::new_with_cache_capacity(
        reviewer.clone() as Arc<dyn WebhookTokenReviewer>,
        Duration::from_secs(60),
        Duration::from_secs(10),
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        2,
    );

    assert!(auth.authenticate("token-a").await.is_some());
    assert!(auth.authenticate("token-b").await.is_some());
    assert!(auth.authenticate("token-c").await.is_some());
    assert_eq!(reviewer.call_count(), 3);

    assert!(auth.authenticate("token-a").await.is_some());
    assert_eq!(
        reviewer.call_count(),
        4,
        "oldest token should be evicted when cache reaches capacity"
    );
}

// ─── WebhookAuth::authenticate unit tests ──────────────────────────────────

#[tokio::test]
async fn test_authenticate_success_returns_identity() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("webhook-user")))));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("valid-token").await;
    assert!(result.is_some());
    let identity = result.unwrap().unwrap();
    assert_eq!(identity.username, "webhook-user");
    assert!(identity.groups.contains(&"developers".to_string()));
    assert!(identity.groups.contains(&"viewers".to_string()));
    assert!(
        identity
            .groups
            .contains(&"system:authenticated".to_string())
    );
    assert_eq!(identity.uid, Some("uid-webhook-user".to_string()));
    assert!(
        identity
            .extra
            .contains(&("org".to_string(), "engineering".to_string()))
    );
}

#[tokio::test]
async fn test_authenticate_not_authenticated_returns_error() {
    let reviewer = reviewer_arc(Ok(Some(unauth_status())));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("bad-token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("not authenticated"), "got: {msg}");
}

#[tokio::test]
async fn test_authenticate_no_status_returns_error() {
    let reviewer = reviewer_arc(Ok(None));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("no status"), "got: {msg}");
}

#[tokio::test]
async fn test_authenticate_webhook_error_returns_error() {
    let reviewer = reviewer_arc(Err("connection refused".to_string()));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("connection refused"), "got: {msg}");
}

#[tokio::test]
async fn test_authenticate_authenticated_no_user_returns_none() {
    let status = TokenReviewStatus {
        authenticated: true,
        user: None,
        error: None,
        audiences: vec![],
    };
    let reviewer = reviewer_arc(Ok(Some(status)));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("token").await;
    assert!(
        result.is_none(),
        "authenticated without user info returns None"
    );
}

#[tokio::test]
async fn test_authenticate_with_webhook_error_field() {
    let status = TokenReviewStatus {
        authenticated: false,
        user: None,
        error: Some("token is expired".to_string()),
        audiences: vec![],
    };
    let reviewer = reviewer_arc(Ok(Some(status)));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("expired-token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("expired"),
        "error field should appear in error message, got: {msg}"
    );
}

#[tokio::test]
async fn test_authenticate_extra_fields_preserved_in_identity() {
    let user = TokenReviewUser {
        username: "jane".to_string(),
        uid: Some("u-1".to_string()),
        groups: vec!["dev".to_string()],
        extra: vec![
            ("org".to_string(), "eng".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ],
    };
    let reviewer = reviewer_arc(Ok(Some(auth_status(user))));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("token").await;
    let identity = result.unwrap().unwrap();
    assert_eq!(identity.extra.len(), 2);
    assert!(
        identity
            .extra
            .contains(&("org".to_string(), "eng".to_string()))
    );
    assert!(
        identity
            .extra
            .contains(&("region".to_string(), "us-east-1".to_string()))
    );
}

#[tokio::test]
async fn test_authenticate_error_takes_precedence_over_authenticated() {
    // K8s spec: status.error takes precedence over status.authenticated.
    // If error is set, the webhook call is treated as failed regardless.
    let status = TokenReviewStatus {
        authenticated: true,
        user: Some(test_user("bob")),
        error: Some("backend authentication service unavailable".to_string()),
        audiences: vec![],
    };
    let reviewer = reviewer_arc(Ok(Some(status)));
    let auth = make_cached_auth(reviewer, Duration::from_secs(60), Duration::from_secs(10));

    let result = auth.authenticate("token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("backend authentication service unavailable"),
        "error should take precedence over authenticated=true, got: {msg}"
    );
}

#[tokio::test]
async fn test_authenticate_custom_audience_rejects_empty_response_audiences() {
    let status = TokenReviewStatus {
        authenticated: true,
        user: Some(test_user("alice")),
        error: None,
        audiences: vec![],
    };
    let reviewer = reviewer_arc(Ok(Some(status)));
    let auth = WebhookAuth::new(
        reviewer,
        Duration::from_secs(60),
        Duration::from_secs(10),
        vec!["custom-audience".to_string()],
    );

    let result = auth.authenticate("token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("audiences"), "got: {msg}");
}

// ─── try_webhook_auth integration tests ────────────────────────────────────

#[tokio::test]
async fn test_try_webhook_auth_no_auth_returns_none() {
    let result = try_webhook_auth(&None, "token").await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_try_webhook_auth_success_returns_identity() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("w-user")))));
    let auth = Arc::new(make_cached_auth(
        reviewer,
        Duration::from_secs(60),
        Duration::from_secs(10),
    ));

    let result = try_webhook_auth(&Some(auth), "valid-token").await;
    assert!(result.is_some());
    let identity = result.unwrap().unwrap();
    assert_eq!(identity.username, "w-user");
    assert!(identity.groups.contains(&"developers".to_string()));
}

#[tokio::test]
async fn test_try_webhook_auth_unauthorized_returns_error() {
    let reviewer = reviewer_arc(Ok(Some(unauth_status())));
    let auth = Arc::new(make_cached_auth(
        reviewer,
        Duration::from_secs(60),
        Duration::from_secs(10),
    ));

    let result = try_webhook_auth(&Some(auth), "bad-token").await;
    assert!(result.is_some());
    assert!(result.unwrap().is_err());
}

#[tokio::test]
async fn test_try_webhook_auth_request_error_returns_error() {
    let reviewer = reviewer_arc(Err("timeout".to_string()));
    let auth = Arc::new(make_cached_auth(
        reviewer,
        Duration::from_secs(60),
        Duration::from_secs(10),
    ));

    let result = try_webhook_auth(&Some(auth), "token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("timeout"), "got: {msg}");
}

// ─── Config tests ──────────────────────────────────────────────────────────

#[test]
fn test_build_webhook_auth_empty_url_returns_none() {
    let config = Some(WebhookAuthConfig {
        url: String::new(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec![],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });
    assert!(build_webhook_auth(config).unwrap().is_none());
}

#[test]
fn test_build_webhook_auth_none_returns_none() {
    assert!(build_webhook_auth(None).unwrap().is_none());
}

#[test]
fn test_build_webhook_auth_with_url_returns_some() {
    let config = Some(WebhookAuthConfig {
        url: "https://auth-webhook:8443/token".to_string(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });
    assert!(build_webhook_auth(config).unwrap().is_some());
}

#[test]
fn test_build_webhook_auth_rejects_http_url() {
    let config = Some(WebhookAuthConfig {
        url: "http://auth-webhook:8080/token".to_string(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });

    let err = match build_webhook_auth(config) {
        Ok(_) => panic!("http webhook URLs must be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("https"), "unexpected error: {msg}");
}

#[test]
fn test_build_webhook_auth_errors_for_invalid_ca_bundle() {
    let config = Some(WebhookAuthConfig {
        url: "https://auth-webhook:8443/token".to_string(),
        ca_bundle: Some(
            "-----BEGIN CERTIFICATE-----\nnot-base64\n-----END CERTIFICATE-----\n".to_string(),
        ),
        client_cert: None,
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });

    let err = match build_webhook_auth(config) {
        Ok(_) => panic!("invalid webhook CA bundles must be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("CA certificate"), "unexpected error: {msg}");
}

#[test]
fn test_build_webhook_auth_errors_for_partial_client_identity() {
    let config = Some(WebhookAuthConfig {
        url: "https://auth-webhook:8443/token".to_string(),
        ca_bundle: None,
        client_cert: Some(
            "-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----\n".to_string(),
        ),
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });

    let err = match build_webhook_auth(config) {
        Ok(_) => panic!("partial webhook mTLS identity must be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("certificate and key"),
        "unexpected error: {msg}"
    );
}

#[tokio::test]
async fn test_build_webhook_auth_from_config_reads_ca_bundle_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    let ca_path = temp_dir.path().join("webhook-ca.pem");
    let cert =
        rcgen::generate_simple_self_signed(vec!["auth-webhook.example.com".to_string()]).unwrap();
    std::fs::write(&ca_path, cert.cert.pem()).unwrap();

    let mut config = crate::KlightsConfig::from_env().unwrap();
    config.webhook_auth_url = Some("https://auth-webhook.example.com/token".to_string());
    config.webhook_auth_ca_bundle = Some(ca_path.to_string_lossy().into_owned());

    let supervisor = crate::task_supervisor::TaskSupervisor::new(
        crate::task_supervisor::TaskCategoryConfig::default(),
    );
    let auth = build_webhook_auth_from_config(&config, &supervisor)
        .await
        .unwrap();

    assert!(auth.is_some());
}

#[test]
fn test_authenticated_identity_webhook_constructor() {
    let id = AuthenticatedIdentity::webhook(
        "webhook-user".to_string(),
        vec!["developers".to_string()],
        Some("uid-1".to_string()),
        vec![("org".to_string(), "eng".to_string())],
    );
    assert_eq!(id.username, "webhook-user");
    assert!(id.groups.contains(&"developers".to_string()));
    assert!(id.groups.contains(&"system:authenticated".to_string()));
    assert_eq!(id.uid, Some("uid-1".to_string()));
    assert!(id.extra.contains(&("org".to_string(), "eng".to_string())));
}
