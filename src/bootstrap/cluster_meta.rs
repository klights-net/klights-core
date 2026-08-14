//! Cluster identity metadata and Kubernetes bootstrap-token management.
//!
//! On first leader-compatible boot, this module generates and persists:
//! - `cluster_id`: a stable UUID identifying the cluster
//! - `leader_epoch`: starts at 0, increments only on explicit promotion
//!
//! These values are stored in the `_klights_meta` table via
//! `DatastoreBackend` metadata methods. Node bootstrap tokens are Kubernetes
//! `bootstrap.kubernetes.io/token` Secrets in `kube-system`.

#[cfg(test)]
use anyhow::Result;
#[cfg(test)]
use klights_cluster_core::{ClusterMembership, ClusterMetadata};

/// Generate a new random cluster ID (UUID v4).
#[cfg(test)]
fn generate_cluster_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
use crate::datastore::backend::DatastoreBackend;

/// Initialize cluster metadata on first leader-compatible boot.
///
/// If the metadata already exists, this is a no-op (metadata persists
/// across restarts). If any key is missing, it is created.
/// T7.1: production seed call-sites now use raft-backed
/// `EnsureClusterMetadata` command. Joining controlplanes receive
/// cluster state from raft, not local writes. Function retained for
/// test helpers that exercise the direct-backend code path.
#[cfg(test)]
pub async fn ensure_cluster_metadata(db: &dyn DatastoreBackend) -> Result<()> {
    // Check if cluster_id already exists
    let existing = db
        .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
        .await?;

    if existing.is_none() {
        let cluster_id = generate_cluster_id();

        db.set_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY, &cluster_id)
            .await?;
        db.set_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY, "0")
            .await?;

        tracing::info!(
            cluster_id = %cluster_id,
            "initialized cluster identity on first boot"
        );
    }

    let _ = crate::bootstrap::bootstrap_token::ensure_bootstrap_tokens(db).await?;

    Ok(())
}

/// Read the current cluster metadata.
///
/// Returns an error if metadata has not been initialized.
#[cfg(test)]
pub async fn read_cluster_metadata(db: &dyn DatastoreBackend) -> Result<ClusterMetadata> {
    let cluster_id = db
        .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("cluster_id not initialized"))?;

    let leader_epoch: i64 = db
        .get_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY)
        .await?
        .unwrap_or_else(|| "0".to_string())
        .parse()
        .unwrap_or(0);

    let current_rv = db.get_current_resource_version().await?;

    Ok(ClusterMetadata {
        cluster_id,
        leader_epoch,
        current_rv,
    })
}

#[cfg(test)]
pub async fn write_cluster_membership(
    db: &dyn DatastoreBackend,
    membership: &ClusterMembership,
) -> Result<()> {
    db.set_klights_meta(
        klights_cluster_store::CLUSTER_ID_META_KEY,
        &membership.cluster_id,
    )
    .await?;
    db.set_klights_meta(
        klights_cluster_store::RAFT_VOTERS_META_KEY,
        &serde_json::to_string(&membership.voters)?,
    )
    .await?;
    db.set_klights_meta(
        klights_cluster_store::RAFT_TERM_META_KEY,
        &membership.term.to_string(),
    )
    .await?;
    db.set_klights_meta(
        klights_cluster_store::RAFT_LEADER_HINT_META_KEY,
        membership.leader_hint.as_deref().unwrap_or(""),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub async fn read_cluster_membership(db: &dyn DatastoreBackend) -> Result<ClusterMembership> {
    let cluster_id = db
        .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
        .await?
        .ok_or_else(|| anyhow::anyhow!("cluster_id not initialized"))?;
    let voters = db
        .get_klights_meta(klights_cluster_store::RAFT_VOTERS_META_KEY)
        .await?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()?
        .unwrap_or_default();
    let term = db
        .get_klights_meta(klights_cluster_store::RAFT_TERM_META_KEY)
        .await?
        .unwrap_or_else(|| "0".to_string())
        .parse()
        .unwrap_or(0);
    let leader_hint = db
        .get_klights_meta(klights_cluster_store::RAFT_LEADER_HINT_META_KEY)
        .await?
        .filter(|hint| !hint.is_empty());

    Ok(ClusterMembership {
        cluster_id,
        voters,
        term,
        leader_hint,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_cluster_id_is_uuid_v4() {
        let id = generate_cluster_id();
        let parsed = uuid::Uuid::parse_str(&id).expect("must be valid UUID");
        assert_eq!(parsed.get_version(), Some(uuid::Version::Random));
    }

    #[test]
    fn generate_cluster_id_is_unique() {
        let a = generate_cluster_id();
        let b = generate_cluster_id();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn ensure_cluster_metadata_creates_on_first_boot() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();

        // Before ensure, no metadata
        assert!(
            db.get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
                .await
                .unwrap()
                .is_none()
        );

        ensure_cluster_metadata(&db).await.unwrap();

        // After ensure, all keys exist
        let cluster_id = db
            .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap();
        assert!(cluster_id.is_some());
        assert!(uuid::Uuid::parse_str(&cluster_id.unwrap()).is_ok());

        let epoch = db
            .get_klights_meta(klights_cluster_store::LEADER_EPOCH_META_KEY)
            .await
            .unwrap();
        assert_eq!(epoch.as_deref(), Some("0"));

        let secrets = db
            .list_resources(
                "v1",
                "Secret",
                Some("kube-system"),
                klights_cluster_store::ResourceListOptions::all(),
            )
            .await
            .unwrap();
        let names = secrets
            .items
            .iter()
            .map(|secret| secret.name.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            names.contains(klights_auth::bootstrap_token::WORKER_BOOTSTRAP_TOKEN_SECRET_NAME),
            "first boot must create the worker bootstrap token Secret"
        );
        assert!(
            names.contains(klights_auth::bootstrap_token::CONTROLPLANE_BOOTSTRAP_TOKEN_SECRET_NAME),
            "first boot must create the controlplane bootstrap token Secret"
        );
        assert!(
            !names
                .iter()
                .any(|name| name.starts_with("bootstrap-token-")),
            "first boot must not create random-suffix bootstrap token Secrets"
        );
    }

    #[tokio::test]
    async fn ensure_cluster_metadata_idempotent_on_restart() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();

        ensure_cluster_metadata(&db).await.unwrap();
        let first_id = db
            .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .unwrap();
        let first_worker = crate::bootstrap::bootstrap_token::ensure_worker_bootstrap_token(&db)
            .await
            .unwrap();
        let first_controlplane =
            crate::bootstrap::bootstrap_token::ensure_controlplane_bootstrap_token(&db)
                .await
                .unwrap();

        // Second call should not change values
        ensure_cluster_metadata(&db).await.unwrap();
        let second_id = db
            .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .unwrap();
        let second_worker = crate::bootstrap::bootstrap_token::ensure_worker_bootstrap_token(&db)
            .await
            .unwrap();
        let second_controlplane =
            crate::bootstrap::bootstrap_token::ensure_controlplane_bootstrap_token(&db)
                .await
                .unwrap();

        assert_eq!(
            first_id, second_id,
            "cluster_id must persist across restarts"
        );
        assert_eq!(
            first_worker, second_worker,
            "live worker bootstrap token Secret must be reused across restarts"
        );
        assert_eq!(
            first_controlplane, second_controlplane,
            "live controlplane bootstrap token Secret must be reused across restarts"
        );
    }

    #[tokio::test]
    async fn read_cluster_metadata_returns_initialized_values() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        ensure_cluster_metadata(&db).await.unwrap();

        let meta = read_cluster_metadata(&db).await.unwrap();
        assert!(!meta.cluster_id.is_empty());
        assert_eq!(meta.leader_epoch, 0);
        // current_rv may be 0 or higher depending on metadata writes
        assert!(meta.current_rv >= 0);
    }

    #[tokio::test]
    async fn read_cluster_metadata_fails_before_init() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        let result = read_cluster_metadata(&db).await;
        assert!(result.is_err());
    }

    // T2 step 1: record_leader_boot tests removed alongside the function.
    // Raft's `current_term` subsumes leader_epoch; the bump is unnecessary
    // because the consensus engine handles it on election win.

    #[tokio::test]
    async fn voters_row_round_trips() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        ensure_cluster_metadata(&db).await.unwrap();

        let membership = klights_cluster_core::ClusterMembership {
            cluster_id: db
                .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
                .await
                .unwrap()
                .unwrap(),
            voters: vec!["mn-leader".to_string(), "mn-leader-2".to_string()],
            term: 7,
            leader_hint: Some("mn-leader-2".to_string()),
        };
        write_cluster_membership(&db, &membership).await.unwrap();

        assert_eq!(read_cluster_membership(&db).await.unwrap(), membership);
    }

    #[tokio::test]
    async fn membership_change_commits_update_meta() {
        let db = crate::datastore::sqlite::Datastore::new_in_memory()
            .await
            .unwrap();
        ensure_cluster_metadata(&db).await.unwrap();
        let cluster_id = db
            .get_klights_meta(klights_cluster_store::CLUSTER_ID_META_KEY)
            .await
            .unwrap()
            .unwrap();

        write_cluster_membership(
            &db,
            &klights_cluster_core::ClusterMembership {
                cluster_id: cluster_id.clone(),
                voters: vec!["mn-leader".to_string()],
                term: 1,
                leader_hint: Some("mn-leader".to_string()),
            },
        )
        .await
        .unwrap();
        write_cluster_membership(
            &db,
            &klights_cluster_core::ClusterMembership {
                cluster_id: cluster_id.clone(),
                voters: vec!["mn-leader".to_string(), "mn-leader-2".to_string()],
                term: 2,
                leader_hint: Some("mn-leader-2".to_string()),
            },
        )
        .await
        .unwrap();

        let membership = read_cluster_membership(&db).await.unwrap();
        assert_eq!(membership.cluster_id, cluster_id);
        assert_eq!(membership.voters, vec!["mn-leader", "mn-leader-2"]);
        assert_eq!(membership.term, 2);
        assert_eq!(membership.leader_hint.as_deref(), Some("mn-leader-2"));
    }
}
