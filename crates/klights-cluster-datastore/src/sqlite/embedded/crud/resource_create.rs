//! Resource create — public Kubernetes create path with metadata injection,
//! ServiceAccount volume injection, and UID precondition warning helper.

use super::super::ordinary;
use super::*;

use super::super::create_staged_post_commit;

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
                    Err(anyhow::Error::new(
                        crate::errors::DatastoreError::already_exists("Resource already exists"),
                    ))
                }
                Err(err) => Err(err),
            };
        }

        ensure_resource_type_meta(&mut data, api_version, kind);
        ensure_metadata_identity(&mut data, namespace, name);

        ensure_metadata_create_defaults(&mut data, self.wall_clock.now_utc());

        ensure_pod_status_ip_arrays(&mut data, api_version, kind);
        let uid = ensure_metadata_uid(&mut data);

        let data_bytes = serde_json::to_vec(&data)?;
        // tokio-rusqlite::call closures must be `'static`, so the SQL parameters
        // need owned Strings.  Allocate them once here at the trait boundary.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();

        let namespace_for_db = namespace.map(str::to_string);
        let uid_for_insert = uid.clone();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::create_resource_in_conn(
                    conn,
                    ordinary::CreateResourceInput {
                        api_version: av,
                        kind: k,
                        namespace: namespace_for_db,
                        name: n,
                        uid: uid_for_insert,
                        data: data_bytes,
                    },
                )
            })
            .await;

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
                let _pending = create_staged_post_commit(
                    api_version,
                    kind,
                    namespace,
                    name,
                    rv,
                    "ADDED",
                    data.clone(),
                );
                #[cfg(any(test, feature = "test-support"))]
                self.publish_watch_event(_pending);

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
            Err(tokio_rusqlite::Error::Error(klights_supervisor::DbError::Sqlite(
                rusqlite::Error::SqliteFailure(err, _),
            ))) if err.code == rusqlite::ErrorCode::ConstraintViolation => {
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
                Err(anyhow::Error::new(
                    crate::errors::DatastoreError::already_exists("Resource already exists"),
                ))
            }
            Err(e) => Err(anyhow!("Failed to create resource: {}", e)),
        }
    }
}
