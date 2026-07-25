//! Root-owned projection of process configuration into focused auth inputs.

use anyhow::{Context, Result};
use std::sync::Arc;

pub async fn build_oidc_authenticator(
    config: &crate::KlightsConfig,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<Option<Arc<dyn crate::auth::oidc::OidcValidator>>> {
    let Some(issuer) = config.oidc_issuer_url.as_ref() else {
        return Ok(None);
    };
    if config.oidc_client_id.as_deref().unwrap_or("").is_empty() {
        anyhow::bail!("OIDC client ID is required when OIDC issuer URL is configured");
    }
    let ca_bundle = read_optional_pem_file(
        task_supervisor,
        "oidc_read_ca_bundle",
        "OIDC CA bundle",
        config.oidc_ca_bundle.as_ref(),
    )
    .await?;
    let authenticator =
        crate::auth::oidc::build_oidc_authenticator(Some(crate::auth::oidc::OidcConfig {
            issuer_url: issuer.clone(),
            client_id: config.oidc_client_id.clone().unwrap_or_default(),
            username_claim: config.oidc_username_claim.clone(),
            username_prefix: None,
            groups_claim: config.oidc_groups_claim.clone(),
            groups_prefix: config.oidc_groups_prefix.clone(),
            ca_bundle,
            signing_algs: crate::auth::oidc::default_signing_algs(),
        }))
        .ok_or_else(|| anyhow::anyhow!("invalid OIDC authenticator configuration"))?;
    Ok(Some(authenticator))
}

pub async fn build_webhook_authenticator(
    config: &crate::KlightsConfig,
    task_supervisor: &klights_supervisor::TaskSupervisor,
) -> Result<Option<Arc<crate::auth::webhook_auth::WebhookAuth>>> {
    let Some(url) = config.webhook_auth_url.as_ref() else {
        return Ok(None);
    };

    let ca_bundle = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_ca_bundle",
        "webhook auth CA bundle",
        config.webhook_auth_ca_bundle.as_ref(),
    )
    .await?;
    let client_cert = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_client_cert",
        "webhook auth client certificate",
        config.webhook_auth_client_cert.as_ref(),
    )
    .await?;
    let client_key = read_optional_pem_file(
        task_supervisor,
        "webhook_auth_read_client_key",
        "webhook auth client key",
        config.webhook_auth_client_key.as_ref(),
    )
    .await?;

    crate::auth::webhook_auth::build_webhook_auth(Some(
        crate::auth::webhook_auth::WebhookAuthConfig {
            url: url.clone(),
            ca_bundle,
            client_cert,
            client_key,
            audiences: config
                .webhook_auth_audiences
                .split(',')
                .map(str::trim)
                .filter(|audience| !audience.is_empty())
                .map(ToString::to_string)
                .collect(),
            cache_authorized_ttl_secs: config.webhook_auth_cache_authorized_ttl_secs,
            cache_unauthorized_ttl_secs: config.webhook_auth_cache_unauthorized_ttl_secs,
        },
    ))
}

async fn read_optional_pem_file(
    task_supervisor: &klights_supervisor::TaskSupervisor,
    label: &'static str,
    description: &'static str,
    path: Option<&String>,
) -> Result<Option<String>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let path_buf = std::path::PathBuf::from(path);
    let key = path_buf.to_string_lossy().into_owned();
    let pem = task_supervisor
        .run_blocking_file_keyed(label, key, move || crate::utils::read_utf8_file(path_buf))
        .await
        .with_context(|| format!("failed to join {description} read"))?
        .with_context(|| format!("failed to read {description} {path}"))?;
    Ok(Some(pem))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn supervisor() -> klights_supervisor::TaskSupervisor {
        klights_supervisor::TaskSupervisor::new(klights_supervisor::TaskCategoryConfig::default())
    }

    #[tokio::test]
    async fn oidc_projection_requires_client_id() {
        let config = crate::KlightsConfig {
            oidc_issuer_url: Some("https://oidc.example.com".to_string()),
            oidc_client_id: None,
            ..crate::KlightsConfig::from_env().expect("test config")
        };

        let error = match build_oidc_authenticator(&config, &supervisor()).await {
            Ok(_) => panic!("missing OIDC client ID must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("client ID"));
    }

    #[tokio::test]
    async fn webhook_projection_reads_explicit_ca_material() {
        let temp_dir = tempfile::tempdir().expect("temporary directory");
        let ca_path = temp_dir.path().join("webhook-ca.pem");
        let cert = rcgen::generate_simple_self_signed(vec!["auth-webhook.example.com".to_string()])
            .expect("test certificate");
        std::fs::write(&ca_path, cert.cert.pem()).expect("write test CA");
        let mut config = crate::KlightsConfig::from_env().expect("test config");
        config.webhook_auth_url = Some("https://auth-webhook.example.com/token".to_string());
        config.webhook_auth_ca_bundle = Some(ca_path.to_string_lossy().into_owned());

        let authenticator = build_webhook_authenticator(&config, &supervisor())
            .await
            .expect("valid webhook configuration");

        assert!(authenticator.is_some());
    }
}
