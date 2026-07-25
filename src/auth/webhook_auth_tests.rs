//! Webhook token auth tests.
//!
//! Tests use a mock `WebhookTokenReviewer` to verify all code paths
//! without network access.
//!
//! Wire format tests (TokenReview JSON round-trip) are in
//! `src/auth/webhook_auth.rs` `#[cfg(test)] mod tests`.

use crate::auth::clock::{MonotonicClock, SystemMonotonicClock};
use crate::auth::identity::AuthenticatedIdentity;
use crate::auth::webhook_auth::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ─── Mock implementation ───────────────────────────────────────────────────

struct MockWebhookReviewer {
    result: Result<Option<TokenReviewStatus>, klights_auth::AuthenticationError>,
    call_count: std::sync::Mutex<usize>,
    seen_audiences: std::sync::Mutex<Vec<Vec<String>>>,
}

impl MockWebhookReviewer {
    fn new(result: Result<Option<TokenReviewStatus>, klights_auth::AuthenticationError>) -> Self {
        Self {
            result,
            call_count: std::sync::Mutex::new(0),
            seen_audiences: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn call_count(&self) -> usize {
        *self.call_count.lock().unwrap()
    }

    fn seen_audiences(&self) -> Vec<Vec<String>> {
        self.seen_audiences.lock().unwrap().clone()
    }
}

struct MutableMonotonicClock {
    now: std::sync::Mutex<Instant>,
}

impl MutableMonotonicClock {
    fn new(now: Instant) -> Self {
        Self {
            now: std::sync::Mutex::new(now),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap();
        *now += duration;
    }
}

impl MonotonicClock for MutableMonotonicClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl WebhookTokenReviewer for MockWebhookReviewer {
    async fn review_token(
        &self,
        _token: &str,
        audiences: &[String],
    ) -> Result<Option<TokenReviewStatus>, klights_auth::AuthenticationError> {
        *self.call_count.lock().unwrap() += 1;
        self.seen_audiences.lock().unwrap().push(audiences.to_vec());
        self.result.clone()
    }
}

struct ClockAdvancingReviewer {
    clock: Arc<MutableMonotonicClock>,
    call_count: std::sync::Mutex<usize>,
}

#[async_trait::async_trait]
impl WebhookTokenReviewer for ClockAdvancingReviewer {
    async fn review_token(
        &self,
        _token: &str,
        _audiences: &[String],
    ) -> Result<Option<TokenReviewStatus>, klights_auth::AuthenticationError> {
        *self.call_count.lock().unwrap() += 1;
        self.clock.advance(Duration::from_secs(59));
        Ok(Some(auth_status(test_user("slow-review"))))
    }
}

fn reviewer_arc(
    result: Result<Option<TokenReviewStatus>, klights_auth::AuthenticationError>,
) -> Arc<MockWebhookReviewer> {
    Arc::new(MockWebhookReviewer::new(result))
}

fn monotonic_clock() -> Arc<dyn MonotonicClock> {
    Arc::new(SystemMonotonicClock)
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
        monotonic_clock(),
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
        monotonic_clock(),
    );

    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(reviewer.call_count(), 1);

    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(reviewer.call_count(), 2);
}

#[tokio::test]
async fn cache_expiry_uses_injected_monotonic_clock() {
    let reviewer = reviewer_arc(Ok(Some(auth_status(test_user("alice")))));
    let clock = Arc::new(MutableMonotonicClock::new(Instant::now()));
    let auth = WebhookAuth::new(
        reviewer.clone() as Arc<dyn WebhookTokenReviewer>,
        Duration::from_secs(60),
        Duration::from_secs(10),
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        clock.clone(),
    );

    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(reviewer.call_count(), 1);

    clock.advance(Duration::from_secs(61));
    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(
        reviewer.call_count(),
        2,
        "caller-owned monotonic time must drive cache expiry"
    );
}

#[tokio::test]
async fn slow_webhook_latency_consumes_cache_ttl() {
    let clock = Arc::new(MutableMonotonicClock::new(Instant::now()));
    let reviewer = Arc::new(ClockAdvancingReviewer {
        clock: clock.clone(),
        call_count: std::sync::Mutex::new(0),
    });
    let auth = WebhookAuth::new(
        reviewer.clone() as Arc<dyn WebhookTokenReviewer>,
        Duration::from_secs(60),
        Duration::from_secs(10),
        vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        clock.clone(),
    );

    assert!(auth.authenticate("token").await.is_some());
    clock.advance(Duration::from_secs(2));
    assert!(auth.authenticate("token").await.is_some());
    assert_eq!(
        *reviewer.call_count.lock().unwrap(),
        2,
        "the pre-call timestamp preserves the accepted cache lifetime semantics"
    );
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
        monotonic_clock(),
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
async fn test_cache_transport_error_is_not_cached() {
    let reviewer = reviewer_arc(Err(klights_auth::AuthenticationError::dependency_failure(
        "timeout",
    )));
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    let _ = auth.authenticate("token").await;
    let _ = auth.authenticate("token").await;
    assert_eq!(
        reviewer.call_count(),
        2,
        "transport errors must be retried instead of cached"
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
        monotonic_clock(),
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
    let reviewer = reviewer_arc(Err(klights_auth::AuthenticationError::dependency_failure(
        "connection refused",
    )));
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
    let auth = make_cached_auth(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
    );

    let result = auth.authenticate("expired-token").await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("expired"),
        "error field should appear in error message, got: {msg}"
    );
    let _ = auth.authenticate("expired-token").await;
    assert_eq!(
        reviewer.call_count(),
        2,
        "unauthenticated webhook status errors must not be cached"
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
async fn test_authenticate_authenticated_takes_precedence_over_error() {
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
    let identity = result.unwrap().unwrap();
    assert_eq!(identity.username, "bob");
}

#[tokio::test]
async fn test_authenticate_custom_audience_accepts_empty_response_audiences() {
    let status = TokenReviewStatus {
        authenticated: true,
        user: Some(test_user("alice")),
        error: None,
        audiences: vec![],
    };
    let reviewer = reviewer_arc(Ok(Some(status)));
    let auth = WebhookAuth::new(
        reviewer.clone(),
        Duration::from_secs(60),
        Duration::from_secs(10),
        vec!["custom-audience".to_string()],
        monotonic_clock(),
    );

    let result = WebhookAuthenticator::authenticate_for_review(&auth, "token", &[]).await;
    assert!(result.is_some());
    let (identity, response_audiences) = result.unwrap().unwrap();
    assert_eq!(identity.username, "alice");
    assert!(response_audiences.is_empty());
    assert_eq!(
        reviewer.seen_audiences(),
        vec![vec!["custom-audience".to_string()]]
    );
}

// ─── try_webhook_auth integration tests ────────────────────────────────────

#[tokio::test]
async fn test_try_webhook_auth_no_auth_returns_none() {
    let result = try_webhook_auth(None, "token").await;
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

    let result = try_webhook_auth(Some(auth.as_ref()), "valid-token").await;
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

    let result = try_webhook_auth(Some(auth.as_ref()), "bad-token").await;
    assert!(result.is_some());
    let error = result.unwrap().expect_err("rejected token must fail");
    assert!(matches!(
        error,
        klights_auth::AuthenticationError::Unauthenticated { .. }
    ));
}

#[tokio::test]
async fn test_try_webhook_auth_request_error_returns_error() {
    let reviewer = reviewer_arc(Err(klights_auth::AuthenticationError::dependency_failure(
        "timeout",
    )));
    let auth = Arc::new(make_cached_auth(
        reviewer,
        Duration::from_secs(60),
        Duration::from_secs(10),
    ));

    let result = try_webhook_auth(Some(auth.as_ref()), "token").await;
    assert!(result.is_some());
    let error = result.unwrap().expect_err("transport failure must fail");
    assert!(matches!(
        error,
        klights_auth::AuthenticationError::DependencyFailure { .. }
    ));
    assert!(error.to_string().contains("timeout"));
}

// ─── Config tests ──────────────────────────────────────────────────────────

#[test]
fn test_prepare_webhook_auth_empty_url_returns_none() {
    let config = Some(WebhookAuthConfig {
        url: String::new(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec![],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });
    assert!(prepare_webhook_auth_config(config).unwrap().is_none());
}

#[test]
fn test_prepare_webhook_auth_none_returns_none() {
    assert!(prepare_webhook_auth_config(None).unwrap().is_none());
}

#[test]
fn test_prepare_webhook_auth_with_url_returns_some() {
    let config = Some(WebhookAuthConfig {
        url: "https://auth-webhook:8443/token".to_string(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });
    assert!(prepare_webhook_auth_config(config).unwrap().is_some());
}

#[test]
fn test_prepare_webhook_auth_rejects_http_url() {
    let config = Some(WebhookAuthConfig {
        url: "http://auth-webhook:8080/token".to_string(),
        ca_bundle: None,
        client_cert: None,
        client_key: None,
        audiences: vec!["https://kubernetes.default.svc.cluster.local".to_string()],
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    });

    let err = match prepare_webhook_auth_config(config) {
        Ok(_) => panic!("http webhook URLs must be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("https"), "unexpected error: {msg}");
}

#[test]
fn test_http_webhook_reviewer_errors_for_invalid_ca_bundle() {
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

    let config = prepare_webhook_auth_config(config).unwrap().unwrap();
    let err = match HttpWebhookTokenReviewer::new(config) {
        Ok(_) => {
            panic!("invalid webhook CA bundles must be rejected by the root-selected client")
        }
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("CA certificate"), "unexpected error: {msg}");
}

#[test]
fn test_http_webhook_reviewer_errors_for_partial_client_identity() {
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

    let config = prepare_webhook_auth_config(config).unwrap().unwrap();
    let err = match HttpWebhookTokenReviewer::new(config) {
        Ok(_) => {
            panic!("partial webhook mTLS identity must be rejected by the selected client")
        }
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("certificate and key"),
        "unexpected error: {msg}"
    );
}

#[test]
fn test_root_composable_webhook_client_accepts_focused_ca_bundle() {
    let cert =
        rcgen::generate_simple_self_signed(vec!["auth-webhook.example.com".to_string()]).unwrap();
    let config = prepare_webhook_auth_config(Some(WebhookAuthConfig {
        url: "https://auth-webhook.example.com/token".to_string(),
        ca_bundle: Some(cert.cert.pem()),
        client_cert: None,
        client_key: None,
        audiences: Vec::new(),
        cache_authorized_ttl_secs: 300,
        cache_unauthorized_ttl_secs: 30,
    }))
    .unwrap()
    .unwrap();
    let reviewer: Arc<dyn WebhookTokenReviewer> =
        Arc::new(HttpWebhookTokenReviewer::new(config.clone()).unwrap());
    let _auth = WebhookAuth::new(
        reviewer,
        Duration::from_secs(config.cache_authorized_ttl_secs),
        Duration::from_secs(config.cache_unauthorized_ttl_secs),
        config.audiences,
        monotonic_clock(),
    );
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
