use super::super::queries;
use super::*;
use crate::datastore::sqlite::create_staged_post_commit;
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
                let rv = Self::next_resource_version_in_conn(conn)?;
                conn.execute(
                    queries::NAMESPACES_INSERT,
                    rusqlite::params![&name_owned, &uid_for_insert, rv, &data_bytes],
                )?;
                super::helpers::insert_watch_event_in_conn(
                    conn,
                    super::helpers::WatchEventInsert::new(
                        "v1",
                        "Namespace",
                        None,
                        &name_owned,
                        rv,
                        "ADDED",
                        &data_bytes,
                    ),
                )?;
                Ok(rv)
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

    pub async fn get_namespace(&self, name: &str) -> Result<Option<Resource>> {
        self.focused_reads.get_namespace(name).await
    }

    pub async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        let list = self
            .focused_reads
            .list_namespaces(label_selector, field_selector)
            .await?;
        Ok(ResourceList {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: list.watch_replay_position,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
    }

    pub async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let list = self
            .focused_reads
            .list_namespaces_page(
                label_selector,
                field_selector,
                page.limit(),
                page.continue_token(),
            )
            .await?;
        Ok(ResourceList {
            items: list.items,
            resource_version: list.resource_version,
            watch_replay_position: list.watch_replay_position,
            continue_token: list.continue_token,
            remaining_item_count: list.remaining_item_count,
        })
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
                let rv = Self::next_resource_version_in_conn(conn)?;
                let rows = conn.execute(
                    queries::NAMESPACE_UPDATE,
                    rusqlite::params![&uid_for_update, rv, &data_bytes, &name_owned, expected_rv],
                )?;
                if rows == 0 {
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                super::helpers::insert_watch_event_in_conn(
                    conn,
                    super::helpers::WatchEventInsert::new(
                        "v1",
                        "Namespace",
                        None,
                        &name_owned,
                        rv,
                        "MODIFIED",
                        &data_bytes,
                    ),
                )?;
                Ok(rv)
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
            Err(tokio_rusqlite::Error::Rusqlite(rusqlite::Error::QueryReturnedNoRows)) => {
                Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "Namespace not found or version conflict",
                )
                .into())
            }
            Err(e) => Err(anyhow!("Failed to update namespace: {}", e)),
        }
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        self.delete_namespace_observed_rv(name).await.map(|_| ())
    }

    pub async fn delete_namespace_observed_rv(&self, name: &str) -> Result<i64> {
        enum NamespaceDeleteResult {
            Deleted { rv: i64, data: Vec<u8> },
            HasRemainingContent,
        }

        let name_owned = name.to_string();
        let result = self
            .db_call("db_query", move |conn| {
                let tx = conn.transaction()?;
                let remaining: i64 = tx.query_row(
                    queries::NAMESPACE_RESOURCES_COUNT,
                    rusqlite::params![&name_owned],
                    |row| row.get(0),
                )?;
                if remaining > 0 {
                    return Ok(NamespaceDeleteResult::HasRemainingContent);
                }
                let namespace_rv = Self::next_resource_version_in_tx(&tx)?;
                let namespace_data: Vec<u8> = tx.query_row(
                    queries::NAMESPACE_GET_DATA,
                    rusqlite::params![&name_owned],
                    |row| row.get(0),
                )?;
                let ns_rows =
                    tx.execute(queries::NAMESPACE_DELETE, rusqlite::params![&name_owned])?;
                if ns_rows == 0 {
                    // Namespace already deleted or never existed —
                    // rollback by NOT committing. (Drop on tx rolls
                    // back.) We surface this as a distinct error so
                    // the caller can map it to 404 vs 5xx.
                    return Err(tokio_rusqlite::Error::Rusqlite(
                        rusqlite::Error::QueryReturnedNoRows,
                    ));
                }
                super::helpers::insert_watch_event_in_conn(
                    &tx,
                    super::helpers::WatchEventInsert::new(
                        "v1",
                        "Namespace",
                        None,
                        &name_owned,
                        namespace_rv,
                        "DELETED",
                        &namespace_data,
                    ),
                )?;
                tx.commit()?;
                Ok(NamespaceDeleteResult::Deleted {
                    rv: namespace_rv,
                    data: namespace_data,
                })
            })
            .await;

        match result {
            Ok(NamespaceDeleteResult::Deleted {
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
            Ok(NamespaceDeleteResult::HasRemainingContent) => {
                Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "Namespace has remaining content",
                )
                .into())
            }
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
                let tx = conn.transaction()?;
                tx.query_row(
                    queries::NAMESPACE_EXISTS,
                    rusqlite::params![&name_owned],
                    |_row| Ok(()),
                )?;
                tx.execute(
                    queries::NAMESPACE_RESOURCES_DELETE_NON_PODS,
                    rusqlite::params![&name_owned],
                )?;
                tx.commit()?;
                Ok(())
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

    pub async fn list_namespace_resources(&self, namespace: &str) -> Result<Vec<Resource>> {
        self.focused_reads.list_namespace_resources(namespace).await
    }

    pub async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.focused_reads
            .list_namespace_resources_of_kind(namespace, kind)
            .await
    }

    pub async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.focused_reads
            .list_namespace_resources_excluding_kind(namespace, kind)
            .await
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        self.focused_reads
            .count_namespace_resources(namespace)
            .await
    }
}
