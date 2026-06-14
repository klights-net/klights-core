//! `RedbResourceStore` — K8s resource CRUD, patch, and status-only updates.
//!
//! All methods operate through `RedbAccessor` for supervised DB access.
//! Owner-reference and watch-event side effects are handled inline.

use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::{Result, anyhow};
use serde_json::Value;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::helpers;
use crate::datastore::redb::key_codec::{lex_next, resource_key, resource_prefix};
use crate::datastore::redb::tables;
use crate::datastore::sqlite::{create_pending_watch_event, publish_pending};
use crate::datastore::types::*;
use crate::watch::WatchBus;

/// Check whether a single JSON value matches a field selector string.
/// Used inside the DB closure where we filter candidates row-by-row.
fn value_matches_field_selector(data: &Value, selector: &str) -> bool {
    if selector.is_empty() {
        return true;
    }
    for part in selector.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (path, expected, is_eq) = if let Some(idx) = part.find("!=") {
            (&part[..idx].trim(), &part[idx + 2..].trim(), false)
        } else if let Some(idx) = part.find('=') {
            (&part[..idx].trim(), &part[idx + 1..].trim(), true)
        } else {
            continue;
        };
        let actual = helpers::resolve_field_path(data, path);
        let effective = actual.as_deref().or(if *expected == "false" {
            Some("false")
        } else {
            None
        });
        let matches = effective == Some(expected);
        if is_eq != matches {
            return false;
        }
    }
    true
}

pub struct RedbResourceStore {
    accessor: Arc<RedbAccessor>,
    watch_bus: Arc<WatchBus>,
}

impl RedbResourceStore {
    pub fn new(accessor: Arc<RedbAccessor>, watch_bus: Arc<WatchBus>) -> Self {
        Self {
            accessor,
            watch_bus,
        }
    }

    /// Run a synchronous redb closure on the DB-category blocking pool.
    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    // -----------------------------------------------------------------------
    // Resource CRUD
    // -----------------------------------------------------------------------

    pub async fn create_res(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        mut data: Value,
    ) -> Result<Resource> {
        helpers::ensure_uid(&mut data);
        let data_clone = Arc::new(data.clone());
        let key = resource_key(av, kind, ns, name);
        let body = serde_json::to_vec(&data)?;
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        let av_res = av_owned.clone();
        let kind_res = kind_owned.clone();
        let ns_res = ns_owned.clone();
        let name_res = name_owned.clone();
        let watch_bus = self.watch_bus.clone();
        let rv = self
            .db_call("create_res", move |db| {
                let res_tbl = if ns_owned.is_some() {
                    tables::RES_NS
                } else {
                    tables::RES_CLUSTER
                };
                let w = db.begin_write()?;
                {
                    let r = w.open_table(res_tbl)?;
                    if r.get(key.as_slice())?.is_some() {
                        return Err(anyhow!("exists"));
                    }
                }
                let rv = helpers::incr_rv(&w)?;
                {
                    let mut r = w.open_table(res_tbl)?;
                    r.insert(key.as_slice(), (rv as u64, body.as_slice()))?;
                }
                {
                    let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                    rvk.insert(rv as u64, key.as_slice())?;
                }
                let ev = serde_json::json!({"apiVersion":av_owned,"kind":kind_owned,"namespace":ns_owned,"name":name_owned,"eventType":"ADDED","data":data});
                helpers::watch_insert(&w, rv, &ev)?;
                helpers::update_owner_table(
                    &w,
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    None,
                    Some(&body),
                )?;
                w.commit()?;
                publish_pending(
                    create_pending_watch_event(
                        &av_owned,
                        &kind_owned,
                        ns_owned.as_deref(),
                        &name_owned,
                        rv,
                        "ADDED",
                        data,
                    ),
                    &watch_bus,
                );
                Ok(rv)
            })
            .await?;
        Ok(Resource {
            id: 0,
            api_version: av_res,
            kind: kind_res,
            namespace: ns_res,
            name: name_res,
            uid: Resource::uid_from_data(&data_clone),
            resource_version: rv,
            data: data_clone,
        })
    }

    pub async fn get_res(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        self.db_call("get_res", move |db| {
            let res_tbl = if ns_owned.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            };
            let r = db.begin_read()?;
            let tbl = r.open_table(res_tbl)?;
            Ok(tbl.get(key.as_slice())?.map(|g| {
                let (rv, body) = g.value();
                let data = helpers::body_val(body);
                Resource {
                    id: 0,
                    api_version: av_owned,
                    kind: kind_owned,
                    namespace: ns_owned.clone(),
                    name: name_owned,
                    uid: Resource::uid_from_data(&data),
                    resource_version: rv as i64,
                    data,
                }
            }))
        })
        .await
    }

    pub async fn update_res(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<Resource> {
        self.update_res_with_preconditions(
            av,
            kind,
            ns,
            name,
            data,
            ResourcePreconditions::resource_version(expected_rv),
        )
        .await
    }

    pub async fn update_res_with_preconditions(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<Resource> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        let watch_bus = self.watch_bus.clone();
        self.db_call("update_res", move |db: &::redb::Database| {
            let mut data = data.clone();
            let key = key.clone();
            let av_o = av_owned.clone();
            let kind_o = kind_owned.clone();
            let ns_o = ns_owned.clone();
            let name_o = name_owned.clone();
            let wbus = watch_bus.clone();
            let preconditions = preconditions.clone();
            let res_tbl = if ns_o.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            };
            let w = db.begin_write()?;
            let (cur_rv, old_body, current) = {
                let table = w.open_table(res_tbl)?;
                let existing = table.get(key.as_slice())?;
                match existing {
                    None => return Err(anyhow!("not found")),
                    Some(g) => {
                        let cur_rv = g.value().0 as i64;
                        let old_body = g.value().1.to_vec();
                        let current =
                            serde_json::from_slice::<Value>(&old_body).unwrap_or(Value::Null);
                        helpers::validate_uid_immutable(&data, &current)?;
                        helpers::validate_resource_preconditions(
                            &preconditions,
                            &current,
                            cur_rv,
                        )?;
                        helpers::preserve_server_metadata_fields_from_existing(
                            &mut data,
                            &current,
                        );
                        (cur_rv, old_body, current)
                    }
                }
            };

            let body = serde_json::to_vec(&data)?;

            if crate::utils::resource_bodies_equal_ignoring_metadata_field(
                &current,
                &data,
                "resourceVersion",
            ) {
                let uid = Resource::uid_from_data(&current);
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "redb_update_resource",
                        api_version: &av_o,
                        kind: &kind_o,
                        namespace: ns_o.as_deref(),
                        name: &name_o,
                        uid: &uid,
                        resource_version: cur_rv,
                        reason: "object unchanged",
                    },
                );
                w.commit()?;
                return Ok(Resource {
                    id: 0,
                    api_version: av_o,
                    kind: kind_o,
                    namespace: ns_o,
                    name: name_o,
                    uid,
                    resource_version: cur_rv,
                    data: Arc::new(current),
                });
            }

            let rv = helpers::incr_rv(&w)?;
            {
                let mut r = w.open_table(res_tbl)?;
                r.insert(key.as_slice(), (rv as u64, body.as_slice()))?;
            }
            {
                let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                rvk.insert(rv as u64, key.as_slice())?;
            }
            let ev = serde_json::json!({"apiVersion":av_o,"kind":kind_o,"namespace":ns_o,"name":name_o,"eventType":"MODIFIED","data":data});
            helpers::watch_insert(&w, rv, &ev)?;
            helpers::update_owner_table(
                &w,
                &av_o,
                &kind_o,
                ns_o.as_deref(),
                &name_o,
                Some(&old_body),
                Some(&body),
            )?;
            w.commit()?;
            publish_pending(
                create_pending_watch_event(
                    &av_o,
                    &kind_o,
                    ns_o.as_deref(),
                    &name_o,
                    rv,
                    "MODIFIED",
                    data.clone(),
                ),
                &wbus,
            );
            Ok(Resource {
                id: 0,
                api_version: av_o,
                kind: kind_o,
                namespace: ns_o,
                name: name_o,
                uid: Resource::uid_from_data(&data),
                resource_version: rv,
                data: Arc::new(data),
            })
        })
        .await
    }

    pub async fn delete_res(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
    ) -> Result<()> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        let watch_bus = self.watch_bus.clone();
        self.db_call("delete_res", move |db| {
            let res_tbl = if ns_owned.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            };
            let w = db.begin_write()?;
            let body: Vec<u8> = {
                let table = w.open_table(res_tbl)?;
                let guard = table.get(key.as_slice())?;
                match guard {
                    None => return Ok(()),
                    Some(g) => g.value().1.to_vec(),
                }
            };
            {
                let mut r = w.open_table(res_tbl)?;
                r.remove(key.as_slice())?;
            }
            let rv = helpers::incr_rv(&w)?;
            {
                let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                rvk.remove(rv as u64)?;
            }
            let data = helpers::body_val(&body);
            let ev = serde_json::json!({"apiVersion":av_owned,"kind":kind_owned,"namespace":ns_owned,"name":name_owned,"eventType":"DELETED","data":data});
            helpers::watch_insert(&w, rv, &ev)?;
            helpers::update_owner_table(
                &w,
                &av_owned,
                &kind_owned,
                ns_owned.as_deref(),
                &name_owned,
                Some(&body),
                None,
            )?;
            w.commit()?;
            publish_pending(
                create_pending_watch_event(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    rv,
                    "DELETED",
                    data,
                ),
                &watch_bus,
            );
            Ok(())
        })
        .await
    }

    pub async fn list_res(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        let ResourceListQuery {
            label_selector,
            field_selector,
            limit,
            continue_token: ct,
        } = query;
        let limit = limit.filter(|lim| *lim > 0);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let ct_owned = ct.map(|s| s.to_string());
        let ls_owned = label_selector.map(|s| s.to_string());
        let fs_owned = field_selector.map(|s| s.to_string());
        let has_selectors = ls_owned.is_some() || fs_owned.is_some();

        let parsed_label_reqs = if let Some(ref sel) = ls_owned {
            Some(crate::label_selector::parse_label_selector(sel)?)
        } else {
            None
        };

        let result = self
            .db_call("list_res", move |db| {
                let r = db.begin_read()?;

                // Selector path: filter inside the scan loop, stop early
                // once we have limit+1 matches. No remainingItemCount.
                if has_selectors {
                    let target = limit
                        .map(|lim| lim.max(1) as usize + 1)
                        .unwrap_or(usize::MAX);

                    let mut matches: Vec<Resource> = Vec::new();

                    let mut scan = |tbl_def: ::redb::TableDefinition<&[u8], (u64, &[u8])>,
                                    ns_filter: Option<&str>|
                     -> Result<()> {
                        let tbl = r.open_table(tbl_def)?;
                        let start_prefix = resource_prefix(&av_owned, &kind_owned, ns_filter);
                        let start = ct_owned
                            .as_deref()
                            .and_then(|name| {
                                lex_next(&resource_key(&av_owned, &kind_owned, ns_filter, name))
                            })
                            .unwrap_or_else(|| {
                                let mut k = start_prefix.clone();
                                k.push(0u8);
                                k
                            });
                        let end = lex_next(&start_prefix).unwrap_or_else(|| {
                            let mut v = start_prefix;
                            v.push(0xFF);
                            v
                        });

                        for e in tbl.range(start.as_slice()..end.as_slice())? {
                            if matches.len() >= target {
                                break;
                            }
                            let (_k, val) = e?;
                            let (rv_u64, body) = val.value();

                            let data: Value = serde_json::from_slice(body).unwrap_or(Value::Null);

                            // Label selector filter
                            if let Some(ref reqs) = parsed_label_reqs {
                                let labels = data
                                    .get("metadata")
                                    .and_then(|m| m.get("labels"))
                                    .and_then(|l| l.as_object());
                                if !reqs.iter().all(|req| req.matches(labels)) {
                                    continue;
                                }
                            }

                            // Field selector filter
                            if let Some(ref fs) = fs_owned
                                && !value_matches_field_selector(&data, fs)
                            {
                                continue;
                            }

                            let item_name = data
                                .get("metadata")
                                .and_then(|m| m.get("name"))
                                .and_then(|n| n.as_str())
                                .unwrap_or("");
                            matches.push(Resource {
                                id: 0,
                                api_version: av_owned.clone(),
                                kind: kind_owned.clone(),
                                namespace: ns_owned.clone(),
                                name: item_name.into(),
                                uid: Resource::uid_from_data(&data),
                                resource_version: rv_u64 as i64,
                                data: Arc::new(data),
                            });
                        }
                        Ok(())
                    };

                    if let Some(ref ns_val) = ns_owned {
                        scan(tables::RES_NS, Some(ns_val))?;
                    } else {
                        scan(tables::RES_NS, None)?;
                        scan(tables::RES_CLUSTER, None)?;
                    }

                    let has_more = limit.is_some() && matches.len() > limit.unwrap() as usize;
                    if has_more {
                        matches.truncate(limit.unwrap() as usize);
                    }
                    let continue_token = if has_more {
                        matches.last().map(|r| r.name.clone())
                    } else {
                        None
                    };
                    // Selector lists omit remainingItemCount — exact count
                    // would require scanning all rows.
                    return Ok(ResourceList {
                        resource_version: 0,
                        items: matches,
                        continue_token,
                        remaining_item_count: None,
                    });
                }

                // Non-selector path: original behavior with exact counts.
                let non_selector_limit = limit.map(|lim| lim as usize);
                let mut items: Vec<Resource> = Vec::new();
                let mut has_more = false;
                let mut remaining_after_page = 0_i64;

                let mut scan = |tbl_def: ::redb::TableDefinition<&[u8], (u64, &[u8])>,
                                ns_filter: Option<&str>|
                 -> Result<()> {
                    let tbl = r.open_table(tbl_def)?;
                    let start_prefix = resource_prefix(&av_owned, &kind_owned, ns_filter);
                    let start = ct_owned
                        .as_deref()
                        .and_then(|name| {
                            lex_next(&resource_key(&av_owned, &kind_owned, ns_filter, name))
                        })
                        .unwrap_or_else(|| {
                            let mut k = start_prefix.clone();
                            k.push(0u8);
                            k
                        });
                    let end = lex_next(&start_prefix).unwrap_or_else(|| {
                        let mut v = start_prefix;
                        v.push(0xFF);
                        v
                    });
                    for e in tbl.range(start.as_slice()..end.as_slice())? {
                        let (_k, val) = e?;
                        if let Some(max) = non_selector_limit
                            && items.len() >= max
                        {
                            has_more = true;
                            remaining_after_page += 1;
                            continue;
                        }
                        let (rv_u64, body) = val.value();
                        let body_owned = body.to_vec();
                        let data: Value =
                            serde_json::from_slice(&body_owned).unwrap_or(Value::Null);
                        let item_name = data
                            .get("metadata")
                            .and_then(|m| m.get("name"))
                            .and_then(|n| n.as_str())
                            .unwrap_or("");
                        items.push(Resource {
                            id: 0,
                            api_version: av_owned.clone(),
                            kind: kind_owned.clone(),
                            namespace: ns_owned.clone(),
                            name: item_name.into(),
                            uid: Resource::uid_from_data(&data),
                            resource_version: rv_u64 as i64,
                            data: Arc::new(data),
                        });
                    }
                    Ok(())
                };

                if let Some(ref ns_val) = ns_owned {
                    scan(tables::RES_NS, Some(ns_val))?;
                } else {
                    scan(tables::RES_NS, None)?;
                    scan(tables::RES_CLUSTER, None)?;
                }
                let continue_token = if has_more {
                    items.last().map(|item| item.name.clone())
                } else {
                    None
                };
                Ok(ResourceList {
                    resource_version: 0,
                    items,
                    continue_token,
                    remaining_item_count: if has_more {
                        Some(remaining_after_page)
                    } else {
                        None
                    },
                })
            })
            .await?;

        Ok(result)
    }

    pub async fn list_res_page(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_res(
            av,
            kind,
            ns,
            ResourceListQuery::new(
                label_selector,
                field_selector,
                page.limit(),
                page.continue_token(),
            ),
        )
        .await
    }

    // -----------------------------------------------------------------------
    // Status-only update
    // -----------------------------------------------------------------------

    pub async fn update_status_only_impl(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<Resource> {
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let name_owned = name.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let watch_bus = self.watch_bus.clone();
        self.db_call("update_status_only", move |db: &::redb::Database| {
            let av_o = av_owned.clone();
            let kind_o = kind_owned.clone();
            let name_o = name_owned.clone();
            let ns_o = ns_owned.clone();
            let wbus = watch_bus.clone();
            let status_c = status.clone();
            let er = expected_rv;
            let av: &str = &av_o;
            let kind: &str = &kind_o;
            let name: &str = &name_o;
            let ns: Option<&str> = ns_o.as_deref();
            let key = resource_key(av, kind, ns, name);
            let res_tbl = if ns_o.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            };
            let w = db.begin_write()?;
            let (old_body, cur_rv) = {
                let tbl = w.open_table(res_tbl)?;
                let g = tbl
                    .get(key.as_slice())?
                    .ok_or_else(|| anyhow!("not found"))?;
                let rv = g.value().0 as i64;
                if let Some(er) = er
                    && er > 0 && rv != er {
                        return Err(crate::datastore::errors::DatastoreError::conflict(
                            format!("rv conflict: expected {er} got {rv}"),
                        )
                        .into());
                    }
                (g.value().1.to_vec(), rv)
            };
            let mut current: Value = serde_json::from_slice(&old_body).unwrap_or(Value::Null);
            if current.get("status") == Some(&status_c) {
                let uid = Resource::uid_from_data(&current);
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "redb_update_status_only",
                        api_version: av,
                        kind,
                        namespace: ns,
                        name,
                        uid: &uid,
                        resource_version: cur_rv,
                        reason: "status unchanged",
                    },
                );
                w.commit()?;
                return Ok(Resource {
                    id: 0,
                    api_version: av.into(),
                    kind: kind.into(),
                    namespace: ns.map(|s| s.into()),
                    name: name.into(),
                    uid,
                    resource_version: cur_rv,
                    data: Arc::new(current),
                });
            }
            if let Some(obj) = current.as_object_mut() {
                obj.insert("status".to_string(), status_c);
            }
            let new_body = serde_json::to_vec(&current)?;
            let rv = helpers::incr_rv(&w)?;
            {
                let mut r = w.open_table(res_tbl)?;
                r.insert(key.as_slice(), (rv as u64, new_body.as_slice()))?;
            }
            {
                let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                rvk.insert(rv as u64, key.as_slice())?;
            }
            let ev = serde_json::json!({"apiVersion":av,"kind":kind,"namespace":ns,"name":name,"eventType":"MODIFIED","data":current});
            helpers::watch_insert(&w, rv, &ev)?;
            w.commit()?;
            publish_pending(
                create_pending_watch_event(
                    av,
                    kind,
                    ns,
                    name,
                    rv,
                    "MODIFIED",
                    current.clone(),
                ),
                &wbus,
            );
            Ok(Resource {
                id: 0,
                api_version: av.into(),
                kind: kind.into(),
                namespace: ns.map(|s| s.into()),
                name: name.into(),
                uid: Resource::uid_from_data(&current),
                resource_version: rv,
                data: Arc::new(current),
            })
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Patch
    // -----------------------------------------------------------------------

    pub async fn patch(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<Option<Resource>> {
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let name_owned = name.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let watch_bus = self.watch_bus.clone();
        self.db_call("patch", move |db| {
            let av: &str = &av_owned;
            let kind: &str = &kind_owned;
            let name: &str = &name_owned;
            let ns: Option<&str> = ns_owned.as_deref();
            let key = resource_key(av, kind, ns, name);
            let res_tbl = if ns_owned.is_some() {
                tables::RES_NS
            } else {
                tables::RES_CLUSTER
            };
            let w = db.begin_write()?;
            let (old_body, current) = {
                let tbl = w.open_table(res_tbl)?;
                match tbl.get(key.as_slice())? {
                    None => return Ok(None),
                    Some(g) => {
                        let (rv, body) = g.value();
                        (Some(body.to_vec()), (rv, body.to_vec()))
                    }
                }
            };
            let mut current_data: Value =
                serde_json::from_slice(&current.1).unwrap_or(Value::Null);
            let before_patch = current_data.clone();
            crate::json_patch::apply_merge_patch(&mut current_data, &patch)?;
            helpers::validate_uid_immutable(&current_data, &before_patch)?;
            if crate::utils::resource_bodies_equal_ignoring_metadata_field(
                &before_patch,
                &current_data,
                "resourceVersion",
            ) {
                let uid = Resource::uid_from_data(&before_patch);
                crate::datastore::diagnostics::log_noop_resource_write(
                    crate::datastore::diagnostics::NoopResourceWrite {
                        operation: "redb_patch_resource_latest",
                        api_version: av,
                        kind,
                        namespace: ns,
                        name,
                        uid: &uid,
                        resource_version: current.0 as i64,
                        reason: "patch result unchanged",
                    },
                );
                w.commit()?;
                return Ok(Some(Resource {
                    id: 0,
                    api_version: av.into(),
                    kind: kind.into(),
                    namespace: ns.map(|s| s.into()),
                    name: name.into(),
                    uid,
                    resource_version: current.0 as i64,
                    data: Arc::new(before_patch),
                }));
            }
            let new_body = serde_json::to_vec(&current_data)?;
            helpers::update_owner_table(
                &w,
                av,
                kind,
                ns,
                name,
                old_body.as_deref(),
                Some(&new_body),
            )?;
            let rv = helpers::incr_rv(&w)?;
            {
                let mut tbl = w.open_table(res_tbl)?;
                tbl.insert(key.as_slice(), (rv as u64, new_body.as_slice()))?;
            }
            {
                let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                rvk.insert(rv as u64, key.as_slice())?;
            }
            let ev = serde_json::json!({"apiVersion":av,"kind":kind,"namespace":ns,"name":name,"eventType":"MODIFIED","data":current_data});
            helpers::watch_insert(&w, rv, &ev)?;
            w.commit()?;
            publish_pending(
                create_pending_watch_event(
                    av,
                    kind,
                    ns,
                    name,
                    rv,
                    "MODIFIED",
                    current_data.clone(),
                ),
                &watch_bus,
            );
            Ok(Some(Resource {
                id: 0,
                api_version: av.into(),
                kind: kind.into(),
                namespace: ns.map(|s| s.into()),
                name: name.into(),
                uid: Resource::uid_from_data(&current_data),
                resource_version: rv,
                data: Arc::new(current_data),
            }))
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Owner reference lookup
    // -----------------------------------------------------------------------

    pub async fn find_owned(
        &self,
        owner_uid: &str,
        ns_filter: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let owner_uid_owned = owner_uid.to_string();
        let ns_filter_owned = ns_filter.map(|s| s.to_string());
        self.db_call("find_owned", move |db| {
            let owner_uid: &str = &owner_uid_owned;
            let ns_filter: Option<&str> = ns_filter_owned.as_deref();
            let prefix = {
                let mut p = owner_uid.as_bytes().to_vec();
                p.push(0);
                p
            };
            let end = lex_next(&prefix).unwrap_or_else(|| {
                let mut v = prefix.clone();
                v.push(0xFF);
                v
            });
            let r = db.begin_read()?;
            let tbl = r.open_table(tables::RESOURCES_BY_OWNER)?;
            let mut items = Vec::new();
            for e in tbl.range(prefix.as_slice()..end.as_slice())? {
                let (_, val) = e?;
                let (rv_u64, body) = val.value();
                let body_owned = body.to_vec();
                let data: Value = serde_json::from_slice(&body_owned).unwrap_or(Value::Null);
                let item_ns = data
                    .get("metadata")
                    .and_then(|m| m.get("namespace"))
                    .and_then(|n| n.as_str());
                if let Some(f) = ns_filter
                    && item_ns != Some(f)
                {
                    continue;
                }
                let item_name = data
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .unwrap_or("");
                let item_av = data
                    .get("apiVersion")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let item_kind = data.get("kind").and_then(|v| v.as_str()).unwrap_or("");
                items.push(Resource {
                    id: 0,
                    api_version: item_av.to_string(),
                    kind: item_kind.to_string(),
                    namespace: item_ns.map(|s| s.to_string()),
                    name: item_name.to_string(),
                    uid: Resource::uid_from_data(&data),
                    resource_version: rv_u64 as i64,
                    data: Arc::new(data),
                });
            }
            Ok(items)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use crate::datastore::redb::accessor::RedbAccessor;
    use crate::datastore::redb::open_boundary;
    use crate::task_supervisor::TaskSupervisor;

    use super::*;

    fn store() -> RedbResourceStore {
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbResourceStore::new(accessor, Arc::new(WatchBus::new(256)))
    }

    // ── tests of code paths NOT covered by cross_backend_tests.rs ──

    #[tokio::test]
    async fn ensure_uid_generates_stable_uuid() {
        let mut data = json!({"metadata":{"name":"x"}});
        helpers::ensure_uid(&mut data);
        let uid1 = data
            .pointer("/metadata/uid")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        assert!(!uid1.is_empty());
        // Second call on same data must not overwrite.
        helpers::ensure_uid(&mut data);
        let uid2 = data.pointer("/metadata/uid").unwrap().as_str().unwrap();
        assert_eq!(uid1, uid2);
    }

    #[test]
    fn field_selector_eq_neq_filters() {
        let items = vec![
            Resource {
                id: 0,
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: None,
                name: "a".into(),
                uid: String::new(),
                resource_version: 1,
                data: Arc::new(json!({"status":{"phase":"Running"}})),
            },
            Resource {
                id: 0,
                api_version: "v1".into(),
                kind: "Pod".into(),
                namespace: None,
                name: "b".into(),
                uid: String::new(),
                resource_version: 2,
                data: Arc::new(json!({"status":{"phase":"Pending"}})),
            },
        ];
        let filtered = helpers::filter_by_field_selector(items, "status.phase=Running");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "a");
    }

    #[test]
    fn field_selector_neq_filters_out() {
        let items = vec![Resource {
            id: 0,
            api_version: "v1".into(),
            kind: "Pod".into(),
            namespace: None,
            name: "a".into(),
            uid: String::new(),
            resource_version: 1,
            data: Arc::new(json!({"status":{"phase":"Running"}})),
        }];
        let filtered = helpers::filter_by_field_selector(items, "status.phase!=Running");
        assert!(filtered.is_empty());
    }

    #[tokio::test]
    async fn list_res_with_continue_token_paginates() {
        let s = store();
        for i in 0..5 {
            s.create_res("v1", "Pod", Some("default"), &format!("p{i}"),
                json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":format!("p{i}"),"namespace":"default"}})).await.unwrap();
        }
        let page1 = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(None, None, Some(3), None),
            )
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 3);
        assert!(page1.continue_token.is_some());
        let page2 = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    None,
                    None,
                    Some(3),
                    page1.continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 2);
        assert!(page2.continue_token.is_none());
    }

    #[tokio::test]
    async fn delete_res_nonexistent_is_noop() {
        let s = store();
        s.delete_res("v1", "Pod", Some("default"), "ghost")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_res_returns_none_for_missing() {
        let s = store();
        let r = s
            .get_res("v1", "Pod", Some("default"), "nope")
            .await
            .unwrap();
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn watch_events_emitted_on_create_update_delete() {
        use crate::watch::events::EventType;
        let db = open_boundary::open_in_memory_blocking().unwrap();
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        let watch_bus = Arc::new(WatchBus::new(256));
        let mut watch_rx = watch_bus.subscribe(crate::watch::WatchTopic::new("v1", "Pod"));
        let s = RedbResourceStore::new(accessor, watch_bus);

        let pod =
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"we","namespace":"default"}});
        let created = s
            .create_res("v1", "Pod", Some("default"), "we", pod)
            .await
            .unwrap();
        let ev1 = watch_rx.recv().await.unwrap();
        assert_eq!(ev1.event_type, EventType::Added);

        s.update_res("v1", "Pod", Some("default"), "we",
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"we","namespace":"default","labels":{"x":"y"}}}),
            created.resource_version).await.unwrap();
        let ev2 = watch_rx.recv().await.unwrap();
        assert_eq!(ev2.event_type, EventType::Modified);

        s.delete_res("v1", "Pod", Some("default"), "we")
            .await
            .unwrap();
        let ev3 = watch_rx.recv().await.unwrap();
        assert_eq!(ev3.event_type, EventType::Deleted);
    }

    #[tokio::test]
    async fn redb_selector_limit_does_not_decode_all_rows() {
        let s = store();
        // Create 20 resources, only 2 match the label selector.
        for i in 0..20 {
            let labels = if i == 5 || i == 15 {
                json!({"app": "web"})
            } else {
                json!({"app": "other"})
            };
            s.create_res(
                "v1",
                "ConfigMap",
                Some("default"),
                &format!("cm-{i:02}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": format!("cm-{i:02}"),
                        "namespace": "default",
                        "labels": labels
                    }
                }),
            )
            .await
            .unwrap();
        }

        let result = s
            .list_res(
                "v1",
                "ConfigMap",
                Some("default"),
                crate::datastore::ResourceListQuery::new(Some("app=web"), None, Some(1), None),
            )
            .await
            .unwrap();

        // First page: exactly 1 item, with continue token for the second match.
        assert_eq!(result.items.len(), 1);
        assert!(
            result.continue_token.is_some(),
            "must have continue when more matches exist beyond the limit"
        );
        assert_eq!(result.items[0].name, "cm-05");

        let page2 = s
            .list_res(
                "v1",
                "ConfigMap",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("app=web"),
                    None,
                    Some(1),
                    result.continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();

        assert_eq!(page2.items.len(), 1);
        assert_eq!(page2.items[0].name, "cm-15");
        assert!(page2.continue_token.is_none());
    }

    #[tokio::test]
    async fn redb_selector_continue_token_returns_next_filtered_page() {
        let s = store();
        // Create 6 matching resources, request pages of 2.
        for i in 0..6 {
            s.create_res(
                "v1",
                "Pod",
                Some("default"),
                &format!("pod-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": format!("pod-{i}"),
                        "namespace": "default",
                        "labels": {"tier": "frontend"}
                    }
                }),
            )
            .await
            .unwrap();
        }

        let page1 = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("tier=frontend"),
                    None,
                    Some(2),
                    None,
                ),
            )
            .await
            .unwrap();
        assert_eq!(page1.items.len(), 2);
        assert!(page1.continue_token.is_some());

        let page2 = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("tier=frontend"),
                    None,
                    Some(2),
                    page1.continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(page2.items.len(), 2);
        assert!(page2.continue_token.is_some());

        let page3 = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    Some("tier=frontend"),
                    None,
                    Some(2),
                    page2.continue_token.as_deref(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(page3.items.len(), 2);
        assert!(page3.continue_token.is_none());
    }

    #[tokio::test]
    async fn redb_residual_selector_late_match_is_not_dropped() {
        let s = store();
        // Create many non-matching resources, then one matching at the end.
        // The match comes after many non-matches by name sort order.
        for i in 0..50 {
            s.create_res(
                "v1",
                "ConfigMap",
                Some("default"),
                &format!("cm-{i:02}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {
                        "name": format!("cm-{i:02}"),
                        "namespace": "default",
                        "labels": {"app": "noise"}
                    }
                }),
            )
            .await
            .unwrap();
        }
        // This comes after cm-49 by lexicographic name ordering.
        s.create_res(
            "v1",
            "ConfigMap",
            Some("default"),
            "cm-zzz",
            json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "name": "cm-zzz",
                    "namespace": "default",
                    "labels": {"app": "target"}
                }
            }),
        )
        .await
        .unwrap();

        let result = s
            .list_res(
                "v1",
                "ConfigMap",
                Some("default"),
                crate::datastore::ResourceListQuery::new(Some("app=target"), None, Some(10), None),
            )
            .await
            .unwrap();

        // The late match must appear — bounded iteration must not stop
        // before reaching it.
        assert_eq!(result.items.len(), 1);
        assert_eq!(result.items[0].name, "cm-zzz");
        assert!(result.continue_token.is_none());
    }

    #[tokio::test]
    async fn redb_selector_pagination_omits_remaining_item_count() {
        let s = store();
        for i in 0..4 {
            s.create_res(
                "v1",
                "Pod",
                Some("default"),
                &format!("pod-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "metadata": {
                        "name": format!("pod-{i}"),
                        "namespace": "default",
                        "labels": {"app": "web"}
                    }
                }),
            )
            .await
            .unwrap();
        }

        let page = s
            .list_res(
                "v1",
                "Pod",
                Some("default"),
                crate::datastore::ResourceListQuery::new(Some("app=web"), None, Some(2), None),
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 2);
        assert!(
            page.remaining_item_count.is_none(),
            "selector pagination must not claim exact remaining count"
        );
        assert!(
            page.continue_token.is_some(),
            "more matches exist, must have continue token"
        );
    }

    #[tokio::test]
    async fn redb_field_selector_with_limit_paginates() {
        let s = store();
        for i in 0..4 {
            s.create_res(
                "v1",
                "Event",
                Some("default"),
                &format!("ev-{i}"),
                json!({
                    "apiVersion": "v1",
                    "kind": "Event",
                    "metadata": {
                        "name": format!("ev-{i}"),
                        "namespace": "default"
                    },
                    "source": {"component": format!("kubelet-{i}")}
                }),
            )
            .await
            .unwrap();
        }

        let page = s
            .list_res(
                "v1",
                "Event",
                Some("default"),
                crate::datastore::ResourceListQuery::new(
                    None,
                    Some("source=kubelet-0"),
                    Some(1),
                    None,
                ),
            )
            .await
            .unwrap();

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.items[0].name, "ev-0");
    }
}
