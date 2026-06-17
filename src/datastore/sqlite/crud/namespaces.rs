use super::super::queries;
use super::*;
use crate::datastore::sqlite::create_pending_watch_event;
use crate::label_selector::LabelSelector;
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
                let pending = create_pending_watch_event(
                    "v1",
                    "Namespace",
                    None,
                    name,
                    rv,
                    "ADDED",
                    data.clone(),
                );
                self.publish_watch_event(pending);

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
        let name_owned = name.to_string();
        let result = self
            .db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::NAMESPACE_GET)?;
                let row = stmt.query_row([&name_owned], |row| {
                    let data_bytes: Vec<u8> = row.get(3)?;
                    let data: Value = serde_json::from_slice(&data_bytes).ok().unwrap_or_default();
                    Ok(Resource {
                        id: 0,
                        api_version: "v1".to_string(),
                        kind: "Namespace".to_string(),
                        namespace: None,
                        name: row.get(0)?,
                        resource_version: row.get(1)?,
                        uid: row.get(2)?,
                        data: std::sync::Arc::new(data),
                    })
                });
                Ok(row)
            })
            .await;

        match result {
            Ok(Ok(resource)) => Ok(Some(resource)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Failed to get namespace: {}", e)),
            Err(e) => Err(anyhow!("Failed to get namespace: {}", e)),
        }
    }

    pub async fn list_namespaces(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
    ) -> Result<ResourceList> {
        let current_rv = self.get_current_resource_version().await?;
        let parsed_label_selector = label_selector
            .map(str::trim)
            .filter(|selector| !selector.is_empty())
            .map(LabelSelector::parse)
            .transpose()
            .map_err(|e| anyhow!("Invalid label selector: {e}"))?;
        let field_selector_owned = field_selector.map(str::to_string);

        self.db_call("db_query", move |conn| {
            let mut query = queries::NAMESPACES_LIST_HEAD.to_string();
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

            // Simple field selector support: metadata.name=foo
            if let Some(ref selector) = field_selector_owned
                && let Some(name_filter) = selector.strip_prefix("metadata.name=")
            {
                query.push_str(" WHERE name = ?");
                params.push(Box::new(name_filter.to_string()));
            }
            query.push_str(" ORDER BY name ASC");

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(param_refs.as_slice(), |row| {
                let data_bytes: Vec<u8> = row.get(3)?;
                let data: Value = serde_json::from_slice(&data_bytes).ok().unwrap_or_default();
                Ok(Resource {
                    id: 0,
                    api_version: "v1".to_string(),
                    kind: "Namespace".to_string(),
                    namespace: None,
                    name: row.get(0)?,
                    resource_version: row.get(1)?,
                    uid: row.get(2)?,
                    data: std::sync::Arc::new(data),
                })
            })?;

            let mut items: Vec<Resource> = Vec::new();
            for row in rows {
                items.push(row?);
            }
            if let Some(selector) = &parsed_label_selector {
                items.retain(|item| selector.matches_resource(&item.data));
            }
            if let Some(selector) = field_selector_owned
                .as_deref()
                .map(str::trim)
                .filter(|selector| !selector.is_empty())
            {
                let conditions = parse_field_selector_conditions(selector);
                if !conditions.is_empty() {
                    items.retain(|item| matches_field_selector_conditions(&item.data, &conditions));
                }
            }
            Ok(ResourceList {
                items,
                resource_version: current_rv,
                continue_token: None,
                remaining_item_count: None,
            })
        })
        .await
        .map_err(|e| anyhow!("Failed to list namespaces: {}", e))
    }

    pub async fn list_namespaces_page(
        &self,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        let list = self.list_namespaces(label_selector, field_selector).await?;
        Ok(page.apply_to_sorted_resource_list(list))
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
                let pending = create_pending_watch_event(
                    "v1",
                    "Namespace",
                    None,
                    name,
                    rv,
                    "MODIFIED",
                    data.clone(),
                );
                self.publish_watch_event(pending);

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
                Err(crate::datastore::errors::DatastoreError::conflict(
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
                let pending =
                    create_pending_watch_event("v1", "Namespace", None, name, rv, "DELETED", data);
                self.publish_watch_event(pending);
                Ok(rv)
            }
            Ok(NamespaceDeleteResult::HasRemainingContent) => {
                Err(crate::datastore::errors::DatastoreError::conflict(
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
        self.list_namespace_resources_filtered(namespace, NamespaceKindFilter::All)
            .await
    }

    pub async fn list_namespace_resources_of_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.list_namespace_resources_filtered(namespace, NamespaceKindFilter::OfKind(kind))
            .await
    }

    pub async fn list_namespace_resources_excluding_kind(
        &self,
        namespace: &str,
        kind: &str,
    ) -> Result<Vec<Resource>> {
        self.list_namespace_resources_filtered(namespace, NamespaceKindFilter::ExcludingKind(kind))
            .await
    }

    async fn list_namespace_resources_filtered(
        &self,
        namespace: &str,
        kind_filter: NamespaceKindFilter<'_>,
    ) -> Result<Vec<Resource>> {
        let ns = namespace.to_string();
        let (sql, kind_param): (&'static str, Option<String>) = match kind_filter {
            NamespaceKindFilter::All => (queries::NAMESPACE_RESOURCES_LIST_ALL, None),
            NamespaceKindFilter::OfKind(k) => (
                queries::NAMESPACE_RESOURCES_LIST_OF_KIND,
                Some(k.to_string()),
            ),
            NamespaceKindFilter::ExcludingKind(k) => (
                queries::NAMESPACE_RESOURCES_LIST_EXCLUDING_KIND,
                Some(k.to_string()),
            ),
        };
        let rows = self
            .db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(sql)?;
                let mapper = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Resource> {
                    let data_bytes: Vec<u8> = row.get(7)?;
                    let data: Value = serde_json::from_slice(&data_bytes).unwrap_or(Value::Null);
                    Ok(Resource {
                        id: row.get(0)?,
                        api_version: row.get(1)?,
                        kind: row.get(2)?,
                        namespace: row.get(3)?,
                        name: row.get(4)?,
                        resource_version: row.get(5)?,
                        uid: row.get(6)?,
                        data: std::sync::Arc::new(data),
                    })
                };
                let mut out = Vec::new();
                match kind_param {
                    None => {
                        let mapped = stmt.query_map(rusqlite::params![&ns], mapper)?;
                        for item in mapped {
                            out.push(item?);
                        }
                    }
                    Some(kind) => {
                        let mapped = stmt.query_map(rusqlite::params![&ns, &kind], mapper)?;
                        for item in mapped {
                            out.push(item?);
                        }
                    }
                }
                Ok(out)
            })
            .await
            .map_err(|e| anyhow!("Failed to list namespace resources: {}", e))?;
        Ok(rows)
    }

    pub async fn count_namespace_resources(&self, namespace: &str) -> Result<i64> {
        let ns = namespace.to_string();
        let count = self
            .db_call("db_query", move |conn| {
                let n: i64 = conn.query_row(
                    queries::NAMESPACE_RESOURCES_COUNT,
                    rusqlite::params![&ns],
                    |row| row.get(0),
                )?;
                Ok(n)
            })
            .await
            .map_err(|e| anyhow!("Failed to count namespace resources: {}", e))?;
        Ok(count)
    }
}

enum NamespaceKindFilter<'a> {
    All,
    OfKind(&'a str),
    ExcludingKind(&'a str),
}
