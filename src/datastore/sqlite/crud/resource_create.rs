//! Resource create — public Kubernetes create path with metadata injection,
//! ServiceAccount volume injection, and UID precondition warning helper.

use super::super::owner_ref_index;
use super::super::queries;
use super::super::selector_index;
use super::helpers::*;
use super::*;
use rusqlite::TransactionBehavior;

use crate::datastore::sqlite::create_pending_watch_event;

impl Datastore {
    pub(super) async fn warn_uid_precondition_mismatch_if_live(
        &self,
        operation: &str,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        expected_uid: &str,
    ) {
        match self.get_resource(api_version, kind, namespace, name).await {
            Ok(Some(resource)) if resource.uid != expected_uid => warn_uid_precondition_mismatch(
                operation,
                api_version,
                kind,
                namespace,
                name,
                expected_uid,
                Some(&resource.uid),
            ),
            Ok(None) => warn_uid_precondition_mismatch(
                operation,
                api_version,
                kind,
                namespace,
                name,
                expected_uid,
                None,
            ),
            _ => {}
        }
    }

    pub async fn create_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        mut data: Value,
    ) -> Result<Resource> {
        if api_version == "v1" && kind == "Namespace" {
            if namespace.is_some() {
                return Err(anyhow!("Namespace is cluster-scoped"));
            }
            return match self.create_namespace(name, data).await {
                Ok(resource) => Ok(resource),
                Err(err) if err.to_string().contains("Namespace already exists") => {
                    Err(anyhow!("Resource already exists (409 Conflict)"))
                }
                Err(err) => Err(err),
            };
        }

        ensure_resource_type_meta(&mut data, api_version, kind);
        ensure_metadata_identity(&mut data, namespace, name);

        ensure_metadata_create_defaults(&mut data);

        // Auto-inject ServiceAccount projected volume for Pods
        if kind == "Pod"
            && api_version == "v1"
            && should_inject_serviceaccount_volume(self, &data, namespace).await
        {
            inject_serviceaccount_volume(&mut data);
        }

        ensure_pod_status_ip_arrays(&mut data, api_version, kind);
        let uid = ensure_metadata_uid(&mut data);

        let data_bytes = serde_json::to_vec(&data)?;
        // tokio-rusqlite::call closures must be `'static`, so the SQL parameters
        // need owned Strings.  Allocate them once here at the trait boundary.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();

        let result = if use_namespaced_table(api_version, kind, &namespace) {
            // Namespaced resource - namespace defaults to "default" if None
            let ns = namespace.unwrap_or("default").to_string();
            let uid = uid.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let rv = Self::next_resource_version_in_tx(&tx)?;
                // created_rv = rv: records the INSERT rv so watch catch-up can
                // distinguish ADDED (created after watch rv) from MODIFIED.
                tx.execute(
                    queries::NAMESPACED_INSERT,
                    rusqlite::params![&av, &k, &ns, &n, &uid, rv, &data_bytes],
                )?;
                selector_index::upsert_index_entries(&tx, &av, &k, &ns, &n, &data_bytes)?;
                owner_ref_index::upsert_owner_refs(&tx, &av, &k, &ns, &n, &data_bytes)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, Some(&ns), &n, rv, "ADDED", &data_bytes),
                )?;
                let rowid = tx.last_insert_rowid();
                tx.commit()?;
                Ok((rowid, rv))
            })
            .await
        } else {
            // Cluster-scoped resource - no namespace column
            let uid = uid.clone();
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let rv = Self::next_resource_version_in_tx(&tx)?;
                // created_rv = rv: records the INSERT rv so watch catch-up can
                // distinguish ADDED (created after watch rv) from MODIFIED.
                tx.execute(
                    queries::CLUSTER_INSERT,
                    rusqlite::params![&av, &k, &n, &uid, rv, &data_bytes],
                )?;
                selector_index::upsert_index_entries(&tx, &av, &k, "", &n, &data_bytes)?;
                owner_ref_index::upsert_owner_refs(&tx, &av, &k, "", &n, &data_bytes)?;
                insert_watch_event_in_conn(
                    &tx,
                    WatchEventInsert::new(&av, &k, None, &n, rv, "ADDED", &data_bytes),
                )?;
                let rowid = tx.last_insert_rowid();
                tx.commit()?;
                Ok((rowid, rv))
            })
            .await
        };

        match result {
            Ok((id, rv)) => {
                if kind == "ControllerRevision" {
                    tracing::info!(
                        target: "klights::datastore::create",
                        kind = kind,
                        ns = ?namespace,
                        name = name,
                        rv = rv,
                        "ControllerRevision stored in DB"
                    );
                }
                let pending = create_pending_watch_event(
                    api_version,
                    kind,
                    namespace,
                    name,
                    rv,
                    "ADDED",
                    data.clone(),
                );
                self.publish_watch_event(pending);

                Ok(Resource {
                    id,
                    api_version: api_version.to_string(),
                    kind: kind.to_string(),
                    namespace: namespace.map(str::to_string),
                    name: name.to_string(),
                    uid: uid.clone(),
                    resource_version: rv,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::SqliteFailure(err, _)))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                if let Ok(Some(live)) = self.get_resource(api_version, kind, namespace, name).await
                    && live.uid != uid
                {
                    warn_uid_precondition_mismatch(
                        "create_resource",
                        api_version,
                        kind,
                        namespace,
                        name,
                        &uid,
                        Some(&live.uid),
                    );
                }
                Err(anyhow!("Resource already exists (409 Conflict)"))
            }
            Err(e) => Err(anyhow!("Failed to create resource: {}", e)),
        }
    }
}
