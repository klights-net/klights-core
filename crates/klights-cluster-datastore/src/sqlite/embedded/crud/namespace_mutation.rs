use super::super::create_staged_post_commit;
use super::super::ordinary;
use super::*;
impl Datastore {
    pub async fn create_namespace(&self, name: &str, mut data: Value) -> Result<Resource> {
        ensure_resource_type_meta(&mut data, "v1", "Namespace");
        ensure_metadata_identity(&mut data, None, name);
        let uid = ensure_metadata_uid(&mut data);
        let data_bytes = serde_json::to_vec(&data)?;
        let name_owned = name.to_string();
        let uid_for_insert = uid.clone();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::create_namespace_in_conn(conn, name_owned, uid_for_insert, data_bytes)
            })
            .await;

        match result {
            Ok(rv) => {
                let _pending = create_staged_post_commit(
                    "v1",
                    "Namespace",
                    None,
                    name,
                    rv,
                    "ADDED",
                    data.clone(),
                );
                #[cfg(test)]
                self.publish_watch_event(_pending);

                Ok(Resource {
                    id: 0, // Not used for namespaces (name is PRIMARY KEY)
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: name.to_string(),
                    uid,
                    resource_version: rv,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::SqliteFailure(err, _)))
                if err.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                Err(anyhow!("Namespace already exists"))
            }
            Err(e) => Err(anyhow!("Failed to create namespace: {}", e)),
        }
    }

    /// Test-only: idempotently insert a namespace row without advancing the
    /// cluster resourceVersion counter and without emitting a watch event, so
    /// RV-asserting and watch-replay tests remain deterministic. Used to make
    /// the standard cluster namespaces present in in-memory test datastores.
    #[cfg(test)]
    pub async fn seed_namespace_no_rv(&self, name: &str) -> Result<()> {
        let data = serde_json::json!({
            "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": name}
        });
        let data_bytes = serde_json::to_vec(&data)?;
        let name_owned = name.to_string();
        let uid = format!("seed-{name}");
        self.db_call("db_query", move |conn| {
            conn.execute(
                "INSERT OR IGNORE INTO namespaces (name, uid, resource_version, data) \
                 VALUES (?1, ?2, 0, ?3)",
                rusqlite::params![&name_owned, &uid, &data_bytes],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow!("seed namespace {}: {}", name, e))?;
        Ok(())
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        mut data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        ensure_resource_type_meta(&mut data, "v1", "Namespace");
        ensure_metadata_identity(&mut data, None, name);
        let uid = ensure_metadata_uid(&mut data);
        let data_bytes = serde_json::to_vec(&data)?;
        let name_owned = name.to_string();
        let uid_for_update = uid.clone();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::update_namespace_in_conn(
                    conn,
                    name_owned,
                    uid_for_update,
                    data_bytes,
                    expected_rv,
                )
            })
            .await;

        match result {
            Ok(rv) => {
                let _pending = create_staged_post_commit(
                    "v1",
                    "Namespace",
                    None,
                    name,
                    rv,
                    "MODIFIED",
                    data.clone(),
                );
                #[cfg(test)]
                self.publish_watch_event(_pending);

                Ok(Resource {
                    id: 0,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: name.to_string(),
                    uid,
                    resource_version: rv,
                    data: std::sync::Arc::new(data),
                })
            }
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => Err(
                crate::errors::DatastoreError::conflict("Namespace not found or version conflict")
                    .into(),
            ),
            Err(e) => Err(anyhow!("Failed to update namespace: {}", e)),
        }
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.delete_namespace_observed_rv(name).await.map(|_| ())
    }

    pub async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        let name_owned = name.to_string();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::delete_namespace_in_conn(conn, name_owned)
            })
            .await;

        match result {
            Ok(ordinary::NamespaceDeleteResult::Deleted {
                rv,
                data: namespace_data,
            }) => {
                let data: Value = serde_json::from_slice(&namespace_data)?;
                let _pending =
                    create_staged_post_commit("v1", "Namespace", None, name, rv, "DELETED", data);
                #[cfg(test)]
                self.publish_watch_event(_pending);
                Ok(rv)
            }
            Ok(ordinary::NamespaceDeleteResult::HasRemainingContent) => Err(
                crate::errors::DatastoreError::conflict("Namespace has remaining content").into(),
            ),
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                Err(anyhow!("Namespace not found"))
            }
            Err(e) => Err(anyhow!("Failed to delete namespace: {}", e)),
        }
    }

    pub async fn delete_namespace_contents(&self, name: &str) -> Result<()> {
        let name_owned = name.to_string();
        let result = self
            .db_call("db_query", move |conn| {
                ordinary::delete_namespace_contents_in_conn(conn, name_owned)
            })
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                Err(anyhow!("Namespace not found"))
            }
            Err(e) => Err(anyhow!("Failed to delete namespace contents: {}", e)),
        }
    }
}
