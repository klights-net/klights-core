//! Root-private datastore composition for auth-owned bootstrap-token policy.

use anyhow::{Result, anyhow};
use klights_auth::bootstrap_token::{
    self, BOOTSTRAP_TOKEN_NAMESPACE, BOOTSTRAP_TOKEN_TTL, BootstrapTokenIdentity,
    BootstrapTokenReadAction, BootstrapTokenScope, BootstrapTokenScopePolicy, BootstrapTokenSecret,
};

use crate::datastore::Resource;

#[async_trait::async_trait]
pub(crate) trait BootstrapTokenStore: Send + Sync {
    async fn get_bootstrap_token_secret(
        &self,
        scope: BootstrapTokenScope,
    ) -> Result<Option<Resource>>;
    async fn create_bootstrap_token_secret(
        &self,
        scope: BootstrapTokenScope,
        data: serde_json::Value,
    ) -> Result<Resource>;
    async fn update_bootstrap_token_secret(
        &self,
        resource: &Resource,
        data: serde_json::Value,
    ) -> Result<Resource>;
}

/// The canonical SQLite store is consumed through this existing narrow auth
/// contract during the transitional backend-removal packets.  It deliberately
/// does not implement the legacy broad backend trait.
#[async_trait::async_trait]
impl BootstrapTokenStore for klights_cluster_datastore::sqlite::embedded::Datastore {
    async fn get_bootstrap_token_secret(
        &self,
        scope: BootstrapTokenScope,
    ) -> Result<Option<Resource>> {
        self.get_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
        )
        .await
    }

    async fn create_bootstrap_token_secret(
        &self,
        scope: BootstrapTokenScope,
        data: serde_json::Value,
    ) -> Result<Resource> {
        self.create_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            scope.secret_name(),
            data,
        )
        .await
    }

    async fn update_bootstrap_token_secret(
        &self,
        resource: &Resource,
        data: serde_json::Value,
    ) -> Result<Resource> {
        self.update_resource(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            &resource.name,
            data,
            resource.resource_version,
        )
        .await
    }
}

pub(crate) fn generate_random_bootstrap_token() -> String {
    use rand_core::RngCore;

    let mut id_entropy = [0u8; 3];
    let mut secret_entropy = [0u8; 8];
    rand_core::OsRng.fill_bytes(&mut id_entropy);
    rand_core::OsRng.fill_bytes(&mut secret_entropy);
    bootstrap_token::generate_bootstrap_token(id_entropy, secret_entropy)
}

pub(crate) struct DatastoreBootstrapTokenValidation {
    resource_reads: Option<std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>>,
}

impl DatastoreBootstrapTokenValidation {
    pub(crate) fn new(
        resource_reads: std::sync::Arc<dyn klights_cluster_store::ClusterResourceRead>,
    ) -> Self {
        Self {
            resource_reads: Some(resource_reads),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        db: &klights_cluster_datastore::sqlite::embedded::Datastore,
    ) -> Self {
        Self::new(db.focused_read_store())
    }
}

impl klights_leader_api::BootstrapTokenValidation for DatastoreBootstrapTokenValidation {
    fn validate_bootstrap_token(
        &self,
        request: klights_leader_api::BootstrapTokenValidationRequest,
    ) -> klights_leader_api::BootstrapTokenValidationFuture<'_> {
        Box::pin(async move {
            let (token, scope) = request.into_parts();
            validate_bootstrap_token_for_scope_with_reads(
                self.resource_reads
                    .as_ref()
                    .expect("focused bootstrap token resource reads")
                    .as_ref(),
                &token,
                scope,
            )
            .await
            .map(|_| ())
            .map_err(|error| {
                klights_leader_api::BootstrapTokenValidationError::rejected(error.to_string())
            })
        })
    }
}

pub(crate) async fn validate_bootstrap_token_for_scope_with_reads(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    token: &str,
    scope: BootstrapTokenScope,
) -> Result<BootstrapTokenIdentity> {
    let _ = bootstrap_token::parse_bootstrap_token(token)?;
    let secret = read_secret_with_reads(resource_reads, scope).await?;
    if let Some(secret) = &secret
        && bootstrap_token::bootstrap_token_matches(secret.data.as_ref(), token).unwrap_or(false)
    {
        return validate_resource(secret, token, Some(scope));
    }

    let other_scope = scope.other();
    if let Some(other_secret) = read_secret_with_reads(resource_reads, other_scope).await?
        && bootstrap_token::bootstrap_token_matches(other_secret.data.as_ref(), token)
            .unwrap_or(false)
    {
        validate_resource(&other_secret, token, Some(other_scope))?;
        return Err(anyhow!("token is not a {}", scope.error_name()));
    }

    match secret {
        Some(secret) => validate_resource(&secret, token, None),
        None => Err(anyhow!("{} not found", scope.error_name())),
    }
}

pub(crate) async fn validate_bootstrap_token_with_reads(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    token: &str,
) -> std::result::Result<BootstrapTokenIdentity, klights_leader_api::ClusterIdentityError> {
    let parsed = bootstrap_token::parse_bootstrap_token(token)
        .map_err(|error| klights_leader_api::ClusterIdentityError::rejected(error.to_string()))?;
    for scope in [
        BootstrapTokenScope::Worker,
        BootstrapTokenScope::Controlplane,
    ] {
        let Some(secret) = read_secret_with_reads(resource_reads, scope)
            .await
            .map_err(|error| {
                klights_leader_api::ClusterIdentityError::dependency_failure(error.to_string())
            })?
        else {
            continue;
        };
        let matches = bootstrap_token::bootstrap_token_matches(secret.data.as_ref(), token)
            .map_err(|error| {
                klights_leader_api::ClusterIdentityError::internal_failure(format!(
                    "bootstrap token Secret {} is malformed: {error}",
                    scope.secret_name()
                ))
            })?;
        if matches {
            return validate_resource(&secret, token, None).map_err(|error| {
                klights_leader_api::ClusterIdentityError::rejected(error.to_string())
            });
        }
    }
    Err(klights_leader_api::ClusterIdentityError::rejected(format!(
        "bootstrap token {} not found",
        parsed.token_id
    )))
}

async fn read_secret_with_reads(
    resource_reads: &dyn klights_cluster_store::ClusterResourceRead,
    scope: BootstrapTokenScope,
) -> Result<Option<Resource>> {
    resource_reads
        .get_resource(klights_cluster_store::ResourceGetRequest::new(
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE.to_string()),
            scope.secret_name(),
        ))
        .await
        .map_err(anyhow::Error::new)
}

pub(crate) async fn ensure_worker_bootstrap_token<S: BootstrapTokenStore + ?Sized>(
    db: &S,
) -> Result<String> {
    ensure_bootstrap_token_for_scope(db, BootstrapTokenScope::Worker).await
}

pub(crate) async fn ensure_controlplane_bootstrap_token(
    db: &(impl BootstrapTokenStore + ?Sized),
) -> Result<String> {
    ensure_bootstrap_token_for_scope(db, BootstrapTokenScope::Controlplane).await
}

pub(crate) async fn ensure_bootstrap_tokens<S: BootstrapTokenStore + ?Sized>(
    db: &S,
) -> Result<(String, String)> {
    let worker = ensure_worker_bootstrap_token(db).await?;
    let controlplane = ensure_controlplane_bootstrap_token(db).await?;
    Ok((worker, controlplane))
}

pub(crate) async fn ensure_bootstrap_token_for_scope(
    db: &(impl BootstrapTokenStore + ?Sized),
    scope: BootstrapTokenScope,
) -> Result<String> {
    if let Some(secret) = read_secret(db, scope).await? {
        let token = bootstrap_token::token_from_secret(secret.data.as_ref())?;
        if validate_resource(&secret, &token, Some(scope)).is_ok() {
            if !bootstrap_token::has_single_token_data_field(secret.data.as_ref()) {
                rewrite_fixed_bootstrap_token_secret_to_single_field(db, &secret, &token).await?;
            }
            return Ok(token);
        }
    }

    let token = generate_random_bootstrap_token();
    write_scoped_bootstrap_token_secret(db, scope, &token, BOOTSTRAP_TOKEN_TTL).await?;
    Ok(token)
}

async fn write_scoped_bootstrap_token_secret(
    db: &(impl BootstrapTokenStore + ?Sized),
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> Result<()> {
    let data = bootstrap_token::build_scoped_bootstrap_token_secret_at(
        scope,
        token,
        ttl,
        time::OffsetDateTime::now_utc(),
    )?;
    if let Some(existing) = read_secret(db, scope).await? {
        db.update_bootstrap_token_secret(&existing, data).await?;
    } else {
        db.create_bootstrap_token_secret(scope, data).await?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn create_scoped_bootstrap_token_secret_with_ttl_for_test(
    db: &(impl BootstrapTokenStore + ?Sized),
    scope: BootstrapTokenScope,
    token: &str,
    ttl: std::time::Duration,
) -> Result<()> {
    write_scoped_bootstrap_token_secret(db, scope, token, ttl).await
}

pub(crate) async fn rotate_bootstrap_token_secret_for_get(
    db: &(impl BootstrapTokenStore + ?Sized),
    resource: &Resource,
) -> Result<Resource> {
    let Some(scope) =
        bootstrap_token::fixed_secret_scope(resource.namespace.as_deref(), resource.name.as_str())
    else {
        return Ok(resource.clone());
    };
    match resource_view(resource).read_action_at(time::OffsetDateTime::now_utc())? {
        BootstrapTokenReadAction::Keep => Ok(resource.clone()),
        BootstrapTokenReadAction::RewriteLegacy => {
            let token = bootstrap_token::token_from_secret(resource.data.as_ref())?;
            rewrite_fixed_bootstrap_token_secret_to_single_field(db, resource, &token).await?;
            read_fixed_secret(db, scope).await
        }
        BootstrapTokenReadAction::Rotate => {
            let token = generate_random_bootstrap_token();
            write_scoped_bootstrap_token_secret(db, scope, &token, BOOTSTRAP_TOKEN_TTL).await?;
            read_fixed_secret(db, scope).await
        }
    }
}

fn validate_resource(
    resource: &Resource,
    token: &str,
    expected_scope: Option<BootstrapTokenScope>,
) -> Result<BootstrapTokenIdentity> {
    bootstrap_token::validate_bootstrap_token_secret_at(
        resource_view(resource),
        token,
        expected_scope,
        time::OffsetDateTime::now_utc(),
    )
}

fn resource_view(resource: &Resource) -> BootstrapTokenSecret<'_> {
    BootstrapTokenSecret {
        namespace: resource.namespace.as_deref(),
        name: resource.name.as_str(),
        data: resource.data.as_ref(),
    }
}

async fn read_secret(
    db: &(impl BootstrapTokenStore + ?Sized),
    scope: BootstrapTokenScope,
) -> Result<Option<Resource>> {
    db.get_bootstrap_token_secret(scope).await
}

async fn read_fixed_secret(
    db: &(impl BootstrapTokenStore + ?Sized),
    scope: BootstrapTokenScope,
) -> Result<Resource> {
    read_secret(db, scope)
        .await?
        .ok_or_else(|| anyhow!("{} not found after rotation", scope.secret_name()))
}

async fn rewrite_fixed_bootstrap_token_secret_to_single_field(
    db: &(impl BootstrapTokenStore + ?Sized),
    resource: &Resource,
    token: &str,
) -> Result<()> {
    let mut data = resource.data.as_ref().clone();
    bootstrap_token::migrate_legacy_token_fields(&mut data, token)?;
    db.update_bootstrap_token_secret(resource, data).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};
    use klights_auth::bootstrap_token::CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME;
    use serde_json::json;
    use std::sync::Arc;

    async fn canonical_sqlite_fixture() -> klights_cluster_datastore::sqlite::embedded::Datastore {
        let supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig::default(),
        ));
        let executor = klights_cluster_datastore::sqlite::open_in_memory(
            supervisor,
            "sqlite:p10-3a-bootstrap-token-tests",
        )
        .await
        .unwrap();
        klights_cluster_datastore::sqlite::embedded::Datastore::new_in_memory_with_watch_and_executor(
            executor,
            crate::bootstrap::composition_adapters::outbox_response_codec_adapter::new_codec(),
            Arc::new(klights_supervisor::SystemWallClock),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn datastore_ensure_reuses_live_tokens_and_keeps_scopes_distinct() {
        let db = canonical_sqlite_fixture().await;
        let first_worker = ensure_worker_bootstrap_token(&db).await.unwrap();
        let first_controlplane = ensure_controlplane_bootstrap_token(&db).await.unwrap();
        assert_eq!(
            ensure_worker_bootstrap_token(&db).await.unwrap(),
            first_worker
        );
        assert_eq!(
            ensure_controlplane_bootstrap_token(&db).await.unwrap(),
            first_controlplane
        );
        assert_ne!(first_worker, first_controlplane);
        validate_bootstrap_token_for_scope_with_reads(
            db.focused_read_store().as_ref(),
            &first_worker,
            BootstrapTokenScope::Worker,
        )
        .await
        .unwrap();
        validate_bootstrap_token_for_scope_with_reads(
            db.focused_read_store().as_ref(),
            &first_controlplane,
            BootstrapTokenScope::Controlplane,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn datastore_ensure_migrates_legacy_split_fields() {
        let db = canonical_sqlite_fixture().await;
        klights_cluster_store::ClusterResourceMutation::create_resource(
            &db,
            "v1",
            "Secret",
            Some(BOOTSTRAP_TOKEN_NAMESPACE),
            CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME,
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "namespace": BOOTSTRAP_TOKEN_NAMESPACE,
                    "name": CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME
                },
                "type": "bootstrap.kubernetes.io/token",
                "data": {
                    "token-id": STANDARD.encode("123456"),
                    "token-secret": STANDARD.encode("fedcba9876543210"),
                    "usage-bootstrap-authentication": STANDARD.encode("true"),
                    "usage-bootstrap-signing": STANDARD.encode("true"),
                    "auth-extra-groups": STANDARD.encode(BootstrapTokenScope::Controlplane.auth_group())
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            ensure_controlplane_bootstrap_token(&db).await.unwrap(),
            "123456.fedcba9876543210"
        );
        let stored = read_secret(&db, BootstrapTokenScope::Controlplane)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            bootstrap_token::token_from_secret(stored.data.as_ref()).unwrap(),
            "123456.fedcba9876543210"
        );
        assert!(stored.data.pointer("/data/token-id").is_none());
        assert!(stored.data.pointer("/data/token-secret").is_none());
    }

    #[tokio::test]
    async fn datastore_get_rotates_near_expiry_and_preserves_fresh_token() {
        let db = canonical_sqlite_fixture().await;
        create_scoped_bootstrap_token_secret_with_ttl_for_test(
            &db,
            BootstrapTokenScope::Worker,
            "abcdef.0123456789abcdef",
            std::time::Duration::from_secs(14 * 60),
        )
        .await
        .unwrap();
        let stale = read_secret(&db, BootstrapTokenScope::Worker)
            .await
            .unwrap()
            .unwrap();
        let rotated = rotate_bootstrap_token_secret_for_get(&db, &stale)
            .await
            .unwrap();
        assert_ne!(
            bootstrap_token::token_from_secret(stale.data.as_ref()).unwrap(),
            bootstrap_token::token_from_secret(rotated.data.as_ref()).unwrap()
        );

        let preserved = rotate_bootstrap_token_secret_for_get(&db, &rotated)
            .await
            .unwrap();
        assert_eq!(
            bootstrap_token::token_from_secret(rotated.data.as_ref()).unwrap(),
            bootstrap_token::token_from_secret(preserved.data.as_ref()).unwrap()
        );
    }
}
