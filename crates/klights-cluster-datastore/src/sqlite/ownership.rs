use anyhow::Result;
use klights_cluster_core::Resource;
use serde_json::Value;

use super::read_queries as queries;
use super::read_store::SqliteReadStore;

impl SqliteReadStore {
    /// Find resources owned by a given owner UID via ownerReferences
    pub async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        tracing::debug!(
            "find_owned_resources: owner_uid={} namespace={:?}",
            owner_uid,
            namespace
        );
        let owner_uid = owner_uid.to_string();
        let namespace_owned = namespace.map(str::to_string);

        // Match owner UID across any ownerReferences[*].uid entry.
        // This is correctness-critical for GC cascade walks; relying on
        // ownerReferences[0] misses valid dependents when the target ownerRef
        // is not in position 0.
        let mut items = Vec::new();

        let namespaced = self
            .read_db_call("db_query", {
                let namespace = namespace_owned.clone();
                let uid = owner_uid.clone();
                move |conn| {
                    let mut query = queries::OWNERSHIP_INDEXED_NAMESPACED_BY_UID.to_string();
                    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(uid)];

                    if let Some(ref ns) = namespace {
                        query.push_str(&format!(" AND r.namespace = ?{}", params.len() + 1));
                        params.push(Box::new(ns.clone()));
                    }

                    let param_refs: Vec<&dyn rusqlite::ToSql> =
                        params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = conn.prepare(&query)?;
                    let rows = stmt.query_map(&param_refs[..], |row| {
                        let data_bytes: Vec<u8> = row.get(7)?;
                        let data: Value = serde_json::from_slice(&data_bytes)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                        Ok(Resource {
                            id: row.get(0)?,
                            api_version: row.get(1)?,
                            kind: row.get(2)?,
                            namespace: Some(row.get(3)?),
                            name: row.get(4)?,
                            resource_version: row.get(5)?,
                            uid: row.get(6)?,
                            data: std::sync::Arc::new(data),
                        })
                    })?;
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                }
            })
            .await?;
        items.extend(namespaced);

        // cluster_resources walk only when namespace is None — namespaced parents
        // never own cluster-scoped children.
        if namespace_owned.is_none() {
            let uid = owner_uid.clone();
            let cluster = self
                .read_db_call("db_query", move |conn| {
                    let mut stmt = conn.prepare(queries::OWNERSHIP_INDEXED_CLUSTER_BY_UID)?;
                    let rows = stmt.query_map([&uid], |row| {
                        let data_bytes: Vec<u8> = row.get(6)?;
                        let data: Value = serde_json::from_slice(&data_bytes)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                        Ok(Resource {
                            id: row.get(0)?,
                            api_version: row.get(1)?,
                            kind: row.get(2)?,
                            namespace: None,
                            name: row.get(3)?,
                            resource_version: row.get(4)?,
                            uid: row.get(5)?,
                            data: std::sync::Arc::new(data),
                        })
                    })?;
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                })
                .await?;
            items.extend(cluster);
        }

        // Keep a defensive filter in Rust to guard malformed rows.
        let filtered: Vec<Resource> = items
            .into_iter()
            .filter(|item| {
                item.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|r| r.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(&owner_uid))
                    })
                    .unwrap_or(false)
            })
            .collect();

        tracing::debug!(
            "find_owned_resources: {} matches for {}",
            filtered.len(),
            owner_uid
        );
        Ok(filtered)
    }

    /// Return all resources of `kind` whose ownerReferences contain
    /// `owner_uid` at any array position.
    pub async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace_owned = namespace.map(str::to_string);
        let owner_uid = owner_uid.to_string();
        let owner_uid_for_filter = owner_uid.clone();

        let rows = self
            .read_db_call("db_query", move |conn| {
                let items = match namespace_owned.as_deref() {
                    Some(ns) => {
                        let mut stmt =
                            conn.prepare(queries::OWNERSHIP_INDEXED_NAMESPACED_BY_KIND_AV_UID)?;
                        let rows =
                            stmt.query_map([&kind, ns, &api_version, &owner_uid], |row| {
                                let data_bytes: Vec<u8> = row.get(7)?;
                                let data: serde_json::Value = serde_json::from_slice(&data_bytes)
                                    .map_err(|e| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(e))
                                })?;
                                Ok(Resource {
                                    id: row.get(0)?,
                                    api_version: row.get(1)?,
                                    kind: row.get(2)?,
                                    namespace: Some(row.get(3)?),
                                    name: row.get(4)?,
                                    resource_version: row.get(5)?,
                                    uid: row.get(6)?,
                                    data: std::sync::Arc::new(data),
                                })
                            })?;
                        let mut items = Vec::new();
                        for row in rows {
                            items.push(row?);
                        }
                        items
                    }
                    None => {
                        let mut stmt =
                            conn.prepare(queries::OWNERSHIP_INDEXED_CLUSTER_BY_KIND_AV_UID)?;
                        let rows = stmt.query_map([&kind, &api_version, &owner_uid], |row| {
                            let data_bytes: Vec<u8> = row.get(6)?;
                            let data: serde_json::Value = serde_json::from_slice(&data_bytes)
                                .map_err(|e| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(e))
                                })?;
                            Ok(Resource {
                                id: row.get(0)?,
                                api_version: row.get(1)?,
                                kind: row.get(2)?,
                                namespace: None,
                                name: row.get(3)?,
                                resource_version: row.get(4)?,
                                uid: row.get(5)?,
                                data: std::sync::Arc::new(data),
                            })
                        })?;
                        let mut items = Vec::new();
                        for row in rows {
                            items.push(row?);
                        }
                        items
                    }
                };
                Ok(items)
            })
            .await?;

        // Defensive filter: confirm owner_uid is actually in ownerReferences.
        let owner_uid_ref = owner_uid_for_filter.as_str();
        let filtered = rows
            .into_iter()
            .filter(|r| {
                r.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|o| o.get("uid").and_then(|u| u.as_str()) == Some(owner_uid_ref))
                    })
                    .unwrap_or(false)
            })
            .collect();

        Ok(filtered)
    }

    /// Find namespaced resources that have an ownerReference with uid=="" AND
    /// matching apiVersion + kind + name. Handles the K8s conformance test
    /// pattern where circular ownerRefs use empty UIDs.
    ///
    /// `owner_api_version` is part of the match so two owners from different
    /// API groups with the same kind/name don't collide. Pass an empty
    /// string to match any apiVersion (legacy behavior, used by callers
    /// that don't yet know the parent's apiVersion).
    pub async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let owner_api_version = owner_api_version.to_string();
        let owner_name = owner_name.to_string();
        let owner_kind = owner_kind.to_string();
        let namespace_owned = namespace.map(str::to_string);

        let owner_api_version_for_filter = owner_api_version.clone();
        let owner_name_for_filter = owner_name.clone();
        let owner_kind_for_filter = owner_kind.clone();

        let namespaced = self
            .read_db_call("db_query", {
                let namespace = namespace_owned.clone();
                let owner_api_version = owner_api_version.clone();
                let owner_name = owner_name.clone();
                let owner_kind = owner_kind.clone();
                move |conn| {
                    let mut query =
                        queries::OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY.to_string();
                    let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                        vec![Box::new(owner_kind), Box::new(owner_name)];

                    if let Some(ref ns) = namespace {
                        query.push_str(&format!(" AND o.namespace = ?{}", params.len() + 1));
                        params.push(Box::new(ns.clone()));
                    }
                    if !owner_api_version.is_empty() {
                        query
                            .push_str(&format!(" AND o.owner_api_version = ?{}", params.len() + 1));
                        params.push(Box::new(owner_api_version.clone()));
                    }

                    let param_refs: Vec<&dyn rusqlite::ToSql> =
                        params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = conn.prepare(&query)?;
                    let rows = stmt.query_map(&param_refs[..], |row| {
                        let data_bytes: Vec<u8> = row.get(7)?;
                        let data: Value = serde_json::from_slice(&data_bytes)
                            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                        Ok(Resource {
                            id: row.get(0)?,
                            api_version: row.get(1)?,
                            kind: row.get(2)?,
                            namespace: Some(row.get(3)?),
                            name: row.get(4)?,
                            resource_version: row.get(5)?,
                            uid: row.get(6)?,
                            data: std::sync::Arc::new(data),
                        })
                    })?;
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                }
            })
            .await?;

        // Precise filter: uid must be "" AND name+kind must match in ownerReferences
        let filtered: Vec<Resource> = namespaced
            .into_iter()
            .filter(|item| {
                item.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|r| r.as_array())
                    .map(|refs| {
                        refs.iter().any(|r| {
                            let uid = r.get("uid").and_then(|u| u.as_str()).unwrap_or("x");
                            let name = r.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let kind = r.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                            let api_version =
                                r.get("apiVersion").and_then(|a| a.as_str()).unwrap_or("");
                            // apiVersion match: empty filter matches any
                            // (legacy callers); otherwise exact match.
                            // Without this two owners from different
                            // groups with the same kind/name would collide
                            // and one's children would be misattributed
                            // to the other.
                            let api_ok = owner_api_version_for_filter.is_empty()
                                || api_version == owner_api_version_for_filter;
                            uid.is_empty()
                                && name == owner_name_for_filter
                                && kind == owner_kind_for_filter
                                && api_ok
                        })
                    })
                    .unwrap_or(false)
            })
            .collect();

        Ok(filtered)
    }
}
