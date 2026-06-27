//! Resource read — get, list (with pagination/selectors), keys-for-scope,
//! cluster-resources, and resource-version helpers.

use super::super::queries;
use super::super::selector_index;
use super::helpers::*;
use super::*;

#[cfg(test)]
pub(crate) struct ListResourcesSnapshotPause {
    target: ListResourcesSnapshotPauseTarget,
    phase: ListResourcesSnapshotPausePhase,
    hit: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    blocking_resume: (std::sync::Mutex<bool>, std::sync::Condvar),
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ListResourcesSnapshotPauseTarget {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    limit: Option<i64>,
    continue_token: Option<String>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ListResourcesSnapshotPausePhase {
    BeforeQuery,
    AfterRows,
}

#[cfg(test)]
static LIST_RESOURCES_SNAPSHOT_PAUSE: std::sync::OnceLock<
    std::sync::Mutex<Option<std::sync::Arc<ListResourcesSnapshotPause>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
impl ListResourcesSnapshotPause {
    pub(crate) async fn wait_for_hit(&self) {
        self.hit.notified().await;
    }

    pub(crate) fn resume(&self) {
        if let Some(slot) = LIST_RESOURCES_SNAPSHOT_PAUSE.get() {
            *slot.lock().expect("list resources pause mutex poisoned") = None;
        }
        let (lock, condvar) = &self.blocking_resume;
        let mut resumed = lock
            .lock()
            .expect("list resources blocking pause mutex poisoned");
        *resumed = true;
        condvar.notify_all();
        self.resume.notify_waiters();
    }

    fn wait_for_resume_blocking(&self) {
        let (lock, condvar) = &self.blocking_resume;
        let mut resumed = lock
            .lock()
            .expect("list resources blocking pause mutex poisoned");
        while !*resumed {
            resumed = condvar
                .wait(resumed)
                .expect("list resources blocking pause mutex poisoned");
        }
    }

    // Test matcher mirrors a list-query signature; a struct would add noise.
    #[allow(clippy::too_many_arguments)]
    fn matches(
        &self,
        phase: ListResourcesSnapshotPausePhase,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> bool {
        self.phase == phase
            && self.target.api_version == api_version
            && self.target.kind == kind
            && self.target.namespace.as_deref() == namespace
            && self.target.label_selector.as_deref() == label_selector
            && self.target.field_selector.as_deref() == field_selector
            && self.target.limit == limit
            && self.target.continue_token.as_deref() == continue_token
    }
}

#[cfg(test)]
async fn maybe_pause_list_resources_snapshot_for_test(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) {
    let pause = LIST_RESOURCES_SNAPSHOT_PAUSE.get().and_then(|slot| {
        slot.lock()
            .expect("list resources pause mutex poisoned")
            .clone()
    });
    if let Some(pause) = pause
        && pause.matches(
            ListResourcesSnapshotPausePhase::BeforeQuery,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    {
        pause.hit.notify_one();
        pause.resume.notified().await;
    }
}

#[cfg(test)]
fn maybe_pause_list_resources_snapshot_after_rows_for_test(
    api_version: &str,
    kind: &str,
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
    limit: Option<i64>,
    continue_token: Option<&str>,
) {
    let pause = LIST_RESOURCES_SNAPSHOT_PAUSE.get().and_then(|slot| {
        slot.lock()
            .expect("list resources pause mutex poisoned")
            .clone()
    });
    if let Some(pause) = pause
        && pause.matches(
            ListResourcesSnapshotPausePhase::AfterRows,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    {
        pause.hit.notify_one();
        pause.wait_for_resume_blocking();
    }
}

fn list_response_resource_version(
    _items: &[Resource],
    current_rv: i64,
    pending_reserved_rv: Option<i64>,
    _request_had_continue: bool,
    _response_has_continue: bool,
) -> i64 {
    // Every list (complete or paginated) normally reports the in-transaction
    // global snapshot resourceVersion. This matches real K8s: the collection
    // revision anchors any follow-up `?watch=true&resourceVersion=<list rv>`
    // to "now", so the server replays nothing for objects already reflected
    // in the list (the kubectl `-w` phantom-pod artifact).
    //
    // Raft proposals reserve the leader's next RV before state-machine apply
    // so every member materializes the same object RV. While such a
    // collection-relevant reservation is in flight, the global metadata RV can
    // be higher than a mutation that is not yet visible in rows/watch history.
    // Cap the list RV just before the earliest unapplied reservation so a
    // follow-up watch cannot skip that eventual ADDED/MODIFIED/DELETED event.
    if let Some(reserved_rv) = pending_reserved_rv.filter(|rv| *rv > 0) {
        return current_rv.min(reserved_rv.saturating_sub(1));
    }
    current_rv
}

fn list_subject_key_prefix(api_version: &str, kind: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(namespace) => format!("{api_version}/{kind}/{namespace}/"),
        None => format!("{api_version}/{kind}/"),
    }
}

fn pending_reserved_rv_for_collection_in_tx(
    tx: &rusqlite::Transaction<'_>,
    subject_prefix: &str,
) -> rusqlite::Result<Option<i64>> {
    tx.query_row(
        "SELECT MIN(reserved_rv) FROM applied_outbox
         WHERE reserved_rv IS NOT NULL
           AND applied_rv IS NULL
           AND length(result_proto) = 0
           AND subject_key LIKE ?1",
        rusqlite::params![format!("{subject_prefix}%")],
        |row| row.get(0),
    )
}

impl Datastore {
    #[cfg(test)]
    pub(crate) fn install_list_resources_snapshot_pause_for_test(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> std::sync::Arc<ListResourcesSnapshotPause> {
        Self::install_list_resources_snapshot_pause_for_test_with_phase(
            ListResourcesSnapshotPausePhase::BeforeQuery,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    }

    #[cfg(test)]
    pub(crate) fn install_list_resources_snapshot_after_rows_pause_for_test(
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> std::sync::Arc<ListResourcesSnapshotPause> {
        Self::install_list_resources_snapshot_pause_for_test_with_phase(
            ListResourcesSnapshotPausePhase::AfterRows,
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
    }

    // Test installer mirrors a list-query signature; a struct would add noise.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    fn install_list_resources_snapshot_pause_for_test_with_phase(
        phase: ListResourcesSnapshotPausePhase,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        limit: Option<i64>,
        continue_token: Option<&str>,
    ) -> std::sync::Arc<ListResourcesSnapshotPause> {
        let pause = std::sync::Arc::new(ListResourcesSnapshotPause {
            target: ListResourcesSnapshotPauseTarget {
                api_version: api_version.to_string(),
                kind: kind.to_string(),
                namespace: namespace.map(str::to_string),
                label_selector: label_selector.map(str::to_string),
                field_selector: field_selector.map(str::to_string),
                limit,
                continue_token: continue_token.map(str::to_string),
            },
            phase,
            hit: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            blocking_resume: (std::sync::Mutex::new(false), std::sync::Condvar::new()),
        });
        let slot = LIST_RESOURCES_SNAPSHOT_PAUSE.get_or_init(|| std::sync::Mutex::new(None));
        *slot.lock().expect("list resources pause mutex poisoned") = Some(pause.clone());
        pause
    }

    pub async fn get_resource(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<Option<Resource>> {
        if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
            return self.get_namespace(name).await;
        }

        // tokio-rusqlite::call closures must be `'static`, so SQL parameters
        // need owned Strings.  Allocate once at the boundary.
        let av = api_version.to_string();
        let k = kind.to_string();
        let n = name.to_string();

        let event_compat = needs_event_v1_compat(api_version, kind);
        let result = if use_namespaced_table(api_version, kind, &namespace) {
            let ns = namespace.unwrap_or("default").to_string();
            self.db_call("db_query", move |conn| {
                let row_mapper = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Resource> {
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
                };
                if event_compat {
                    // K8s Events compat: bridge core/v1 <-> events.k8s.io/v1
                    // for reads. See event_read_api_versions docs.
                    let mut stmt = conn.prepare(queries::NAMESPACED_GET_EVENT_COMPAT)?;
                    Ok(stmt.query_row(rusqlite::params![&k, &ns, &n], row_mapper))
                } else {
                    let mut stmt = conn.prepare(queries::NAMESPACED_GET)?;
                    Ok(stmt.query_row(rusqlite::params![&av, &k, &ns, &n], row_mapper))
                }
            })
            .await
        } else {
            self.db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::CLUSTER_GET)?;
                Ok(stmt.query_row(rusqlite::params![&av, &k, &n], |row| {
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
                }))
            })
            .await
        };

        match result {
            Ok(Ok(resource)) => Ok(Some(resource)),
            Ok(Err(rusqlite::Error::QueryReturnedNoRows)) => Ok(None),
            Ok(Err(e)) => Err(anyhow!("Database error: {}", e)),
            Err(e) => Err(anyhow!("Failed to get resource: {}", e)),
        }
    }

    pub async fn list_resources(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        query: ResourceListQuery<'_>,
    ) -> Result<ResourceList> {
        let ResourceListQuery {
            label_selector,
            field_selector,
            limit,
            continue_token,
        } = query;
        if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
            return self
                .list_namespaces_page(
                    label_selector,
                    field_selector,
                    ListPageRequest::try_new(limit, continue_token.map(str::to_string))?,
                )
                .await;
        }

        #[cfg(test)]
        maybe_pause_list_resources_snapshot_for_test(
            api_version,
            kind,
            namespace,
            label_selector,
            field_selector,
            limit,
            continue_token,
        )
        .await;
        let limit = limit.filter(|lim| *lim > 0);

        // tokio-rusqlite::call closures must be `'static`.
        let av = api_version.to_string();
        let k = kind.to_string();
        let ns_owned = namespace.map(str::to_string);
        let token_owned = continue_token.map(str::to_string);
        let request_had_continue = continue_token.is_some();
        let selector_free_limited =
            label_selector.is_none() && field_selector.is_none() && limit.is_some();

        if selector_free_limited {
            let lim = limit.expect("selector_free_limited implies Some(limit)");
            let fetch_limit = lim;
            let token_for_count = token_owned.clone();
            let ns_for_count = ns_owned.clone();
            let event_compat = needs_event_v1_compat(api_version, kind);
            let (items, total_after_token, current_rv, pending_reserved_rv) =
                if use_namespaced_table(api_version, kind, &namespace) {
                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let (where_head, mut params): (&str, Vec<Box<dyn rusqlite::ToSql>>) =
                            if event_compat {
                                (
                                    queries::NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD,
                                    vec![Box::new(k.clone())],
                                )
                            } else {
                                (
                                    queries::NAMESPACED_LIST_BY_AV_KIND_HEAD,
                                    vec![Box::new(av.clone()), Box::new(k.clone())],
                                )
                            };
                        let mut query = format!("{}{where_head}", queries::NAMESPACED_LIST_HEAD,);
                        if let Some(ref ns_val) = ns_owned {
                            query.push_str(&format!(" AND namespace = ?{}", params.len() + 1));
                            params.push(Box::new(ns_val.clone()));
                        }
                        if let Some(token) = &token_owned {
                            query.push_str(&format!(" AND name > ?{}", params.len() + 1));
                            params.push(Box::new(token.clone()));
                        }

                        let mut count_query =
                            format!("{}{where_head}", queries::NAMESPACED_COUNT_HEAD);
                        let mut count_params: Vec<Box<dyn rusqlite::ToSql>> = if event_compat {
                            vec![Box::new(k.clone())]
                        } else {
                            vec![Box::new(av.clone()), Box::new(k.clone())]
                        };
                        if let Some(ref ns_val) = ns_for_count {
                            count_query
                                .push_str(&format!(" AND namespace = ?{}", count_params.len() + 1));
                            count_params.push(Box::new(ns_val.clone()));
                        }
                        if let Some(token) = &token_for_count {
                            count_query
                                .push_str(&format!(" AND name > ?{}", count_params.len() + 1));
                            count_params.push(Box::new(token.clone()));
                        }

                        query.push_str(&format!(" ORDER BY name LIMIT ?{}", params.len() + 1));
                        params.push(Box::new(fetch_limit));

                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let mut stmt = conn.prepare(&query)?;
                        let rows = stmt.query_map(&param_refs[..], row_to_namespaced_resource)?;
                        // Bounded by `fetch_limit` (LIMIT clause); pre-size to avoid
                        // realloc churn on the common large-page list path.
                        let mut items = Vec::with_capacity(fetch_limit as usize);
                        for row in rows {
                            items.push(row?);
                        }

                        let count_param_refs: Vec<&dyn rusqlite::ToSql> =
                            count_params.iter().map(|p| p.as_ref()).collect();
                        let total_after_token: i64 =
                            conn.query_row(&count_query, &count_param_refs[..], |row| row.get(0))?;
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, ns_owned.as_deref()),
                        )?;

                        Ok((items, total_after_token, current_rv, pending_reserved_rv))
                    })
                    .await?
                } else {
                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let mut query = queries::CLUSTER_LIST_HEAD.to_string();
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(av.clone()), Box::new(k.clone())];
                        if let Some(token) = &token_owned {
                            query.push_str(&format!(" AND name > ?{}", params.len() + 1));
                            params.push(Box::new(token.clone()));
                        }
                        query.push_str(&format!(" ORDER BY name LIMIT ?{}", params.len() + 1));
                        params.push(Box::new(fetch_limit));

                        let mut count_query = queries::CLUSTER_COUNT_HEAD.to_string();
                        let mut count_params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(av.clone()), Box::new(k.clone())];
                        if let Some(token) = &token_for_count {
                            count_query
                                .push_str(&format!(" AND name > ?{}", count_params.len() + 1));
                            count_params.push(Box::new(token.clone()));
                        }

                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let mut stmt = conn.prepare(&query)?;
                        let rows = stmt.query_map(&param_refs[..], row_to_cluster_resource)?;
                        // Bounded by `fetch_limit` (LIMIT clause); pre-size to avoid
                        // realloc churn on the common large-page list path.
                        let mut items = Vec::with_capacity(fetch_limit as usize);
                        for row in rows {
                            items.push(row?);
                        }

                        let count_param_refs: Vec<&dyn rusqlite::ToSql> =
                            count_params.iter().map(|p| p.as_ref()).collect();
                        let total_after_token: i64 =
                            conn.query_row(&count_query, &count_param_refs[..], |row| row.get(0))?;
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, None),
                        )?;
                        Ok((items, total_after_token, current_rv, pending_reserved_rv))
                    })
                    .await?
                };

            let mut next_token = None;
            let mut remaining_item_count = None;
            if total_after_token > lim {
                next_token = items.last().map(|r| r.name.clone());
                remaining_item_count = Some((total_after_token - lim).max(0));
            }
            let response_rv = list_response_resource_version(
                &items,
                current_rv,
                pending_reserved_rv,
                request_had_continue,
                next_token.is_some(),
            );

            return Ok(ResourceList {
                items,
                resource_version: response_rv,
                continue_token: next_token,
                remaining_item_count,
            });
        }

        let selector_limited =
            limit.is_some() && (label_selector.is_some() || field_selector.is_some());
        if selector_limited {
            let lim = limit.expect("selector_limited implies Some(limit)") as usize;
            let label_requirements = if let Some(selector) = label_selector {
                Some(parse_label_selector(selector)?)
            } else {
                None
            };

            // Split field selector into SQL-pushable (metadata.name/namespace)
            // and residual conditions.
            let field_pushdown = field_selector
                .map(split_sql_pushdown_conditions)
                .unwrap_or_default();
            let field_conditions_raw = if field_pushdown.residual_selector.is_empty() {
                Vec::new()
            } else {
                parse_field_selector_conditions(&field_pushdown.residual_selector)
            };

            let event_compat_selector = needs_event_v1_compat(api_version, kind);

            // Build the pushdown separately for each branch because the param
            // offset (base query parameter count) differs between namespaced
            // and cluster paths.
            let (items, current_rv, pending_reserved_rv) = if use_namespaced_table(
                api_version,
                kind,
                &namespace,
            ) {
                // Base param count WITHOUT token/cursor: used for residual
                // cursor batching where the cursor comes after pushdown.
                // The non-residual path adds token back for its offset.
                let base_param_count = if event_compat_selector { 1 } else { 2 }
                    + if ns_owned.is_some() { 1 } else { 0 }
                    + if field_pushdown.sql_name_eq.is_some() {
                        1
                    } else {
                        0
                    }
                    + if field_pushdown.sql_namespace_eq.is_some() {
                        1
                    } else {
                        0
                    };

                let index_pushdown = selector_index::build_selector_pushdown(
                    label_requirements.as_deref().unwrap_or(&[]),
                    &field_conditions_raw,
                    &av,
                    &k,
                    base_param_count,
                    false,
                );

                let has_residual = !index_pushdown.residual_labels.is_empty()
                    || !index_pushdown.residual_fields.is_empty();

                if has_residual {
                    // Bounded cursor batching: advance through candidates in
                    // bounded batches until lim+1 matches are found or no more
                    // candidates remain. The cursor (r.name > ?) comes after the
                    // pushdown clauses so the pushdown param offset is stable.
                    let batch_size = (lim * selector_index::SELECTOR_RESIDUAL_SCAN_FACTOR)
                        .clamp(128, selector_index::SELECTOR_RESIDUAL_MAX_CANDIDATES);

                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        // Build base query without cursor / ORDER BY / LIMIT.
                        let (where_head, mut base_param_strings): (&str, Vec<String>) =
                            if event_compat_selector {
                                (
                                    queries::NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD,
                                    vec![k.clone()],
                                )
                            } else {
                                (
                                    queries::NAMESPACED_LIST_BY_AV_KIND_HEAD,
                                    vec![av.clone(), k.clone()],
                                )
                            };
                        let mut base_query = format!(
                            "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
                             FROM namespaced_resources r {where_head}"
                        );
                        if let Some(ref ns_val) = ns_owned {
                            base_query.push_str(&format!(
                                " AND r.namespace = ?{}",
                                base_param_strings.len() + 1
                            ));
                            base_param_strings.push(ns_val.clone());
                        }
                        if let Some(name_eq) = field_pushdown.sql_name_eq.as_ref() {
                            base_query.push_str(&format!(
                                " AND r.name = ?{}",
                                base_param_strings.len() + 1
                            ));
                            base_param_strings.push(name_eq.clone());
                        }
                        if let Some(ns_eq) = field_pushdown.sql_namespace_eq.as_ref() {
                            base_query.push_str(&format!(
                                " AND r.namespace = ?{}",
                                base_param_strings.len() + 1
                            ));
                            base_param_strings.push(ns_eq.clone());
                        }
                        // Index pushdown clauses (param offset excludes cursor).
                        for clause in &index_pushdown.sql_clauses {
                            base_query.push_str(&format!(" AND {clause}"));
                        }
                        for p in &index_pushdown.sql_params {
                            base_param_strings.push(p.clone());
                        }

                        let residual_labels = &index_pushdown.residual_labels;
                        let residual_fields = &index_pushdown.residual_fields;
                        let mut page_items = Vec::with_capacity(lim + 1);
                        let mut cursor_name = token_owned.clone();

                        loop {
                            let mut query = base_query.clone();
                            let mut param_strings = base_param_strings.clone();
                            if let Some(cursor) = &cursor_name {
                                query.push_str(&format!(
                                    " AND r.name > ?{}",
                                    param_strings.len() + 1
                                ));
                                param_strings.push(cursor.clone());
                            }
                            query.push_str(" ORDER BY r.name");
                            query.push_str(&format!(" LIMIT ?{}", param_strings.len() + 1));

                            let mut params: Vec<Box<dyn rusqlite::ToSql>> = param_strings
                                .iter()
                                .map(|s| Box::new(s.clone()) as Box<dyn rusqlite::ToSql>)
                                .collect();
                            params.push(Box::new(batch_size as i64));
                            let param_refs: Vec<&dyn rusqlite::ToSql> =
                                params.iter().map(|p| p.as_ref()).collect();

                            let mut stmt = conn.prepare(&query)?;
                            let rows =
                                stmt.query_map(&param_refs[..], |row| {
                                    let data_bytes: Vec<u8> = row.get(7)?;
                                    let data: Value =
                                        serde_json::from_slice(&data_bytes).map_err(|e| {
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

                            let mut batch_count = 0usize;
                            let mut last_candidate_name: Option<String> = None;
                            for row in rows {
                                let item = row?;
                                batch_count += 1;
                                last_candidate_name = Some(item.name.clone());
                                if !residual_labels.is_empty()
                                    && !matches_label_requirements(
                                        &item.data,
                                        residual_labels,
                                    )
                                {
                                    continue;
                                }
                                if !residual_fields.is_empty()
                                    && !matches_field_selector_conditions(
                                        &item.data,
                                        residual_fields,
                                    )
                                {
                                    continue;
                                }
                                if page_items.len() <= lim {
                                    page_items.push(item);
                                } else {
                                    break;
                                }
                            }

                            if page_items.len() > lim {
                                break;
                            }
                            if batch_count < batch_size {
                                break;
                            }
                            cursor_name = last_candidate_name;
                        }
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, ns_owned.as_deref()),
                        )?;
                        Ok((page_items, current_rv, pending_reserved_rv))
                    })
                    .await?
                } else {
                    // Fully indexed: single query with limit+1 (cursor comes
                    // before pushdown so rebuild pushdown with token offset).
                    let base_param_count_with_token =
                        base_param_count + if token_owned.is_some() { 1 } else { 0 };
                    let index_pushdown = selector_index::build_selector_pushdown(
                        label_requirements.as_deref().unwrap_or(&[]),
                        &field_conditions_raw,
                        &av,
                        &k,
                        base_param_count_with_token,
                        false,
                    );
                    #[cfg(test)]
                    let pause_av = av.clone();
                    #[cfg(test)]
                    let pause_k = k.clone();
                    #[cfg(test)]
                    let pause_ns = ns_owned.clone();
                    #[cfg(test)]
                    let pause_label_selector = label_selector.map(str::to_string);
                    #[cfg(test)]
                    let pause_field_selector = field_selector.map(str::to_string);
                    #[cfg(test)]
                    let pause_limit = limit;
                    #[cfg(test)]
                    let pause_continue_token = token_owned.clone();
                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let (where_head, mut params): (&str, Vec<Box<dyn rusqlite::ToSql>>) =
                            if event_compat_selector {
                                (
                                    queries::NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD,
                                    vec![Box::new(k.clone())],
                                )
                            } else {
                                (
                                    queries::NAMESPACED_LIST_BY_AV_KIND_HEAD,
                                    vec![Box::new(av.clone()), Box::new(k.clone())],
                                )
                            };
                        let mut query = format!(
                            "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
                             FROM namespaced_resources r {where_head}"
                        );
                        if let Some(ref ns_val) = ns_owned {
                            query.push_str(&format!(" AND r.namespace = ?{}", params.len() + 1));
                            params.push(Box::new(ns_val.clone()));
                        }
                        if let Some(name_eq) = field_pushdown.sql_name_eq.as_ref() {
                            query.push_str(&format!(" AND r.name = ?{}", params.len() + 1));
                            params.push(Box::new(name_eq.clone()));
                        }
                        if let Some(ns_eq) = field_pushdown.sql_namespace_eq.as_ref() {
                            query.push_str(&format!(" AND r.namespace = ?{}", params.len() + 1));
                            params.push(Box::new(ns_eq.clone()));
                        }
                        if let Some(token) = &token_owned {
                            query.push_str(&format!(" AND r.name > ?{}", params.len() + 1));
                            params.push(Box::new(token.clone()));
                        }
                        for clause in &index_pushdown.sql_clauses {
                            query.push_str(&format!(" AND {clause}"));
                        }
                        for p in &index_pushdown.sql_params {
                            params.push(Box::new(p.clone()));
                        }
                        query.push_str(" ORDER BY r.name");
                        query.push_str(&format!(" LIMIT ?{}", params.len() + 1));
                        params.push(Box::new((lim + 1) as i64));

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
                        let mut page_items = Vec::with_capacity(lim);
                        for row in rows {
                            let item = row?;
                            if page_items.len() <= lim {
                                page_items.push(item);
                            } else {
                                break;
                            }
                        }
                        #[cfg(test)]
                        maybe_pause_list_resources_snapshot_after_rows_for_test(
                            &pause_av,
                            &pause_k,
                            pause_ns.as_deref(),
                            pause_label_selector.as_deref(),
                            pause_field_selector.as_deref(),
                            pause_limit,
                            pause_continue_token.as_deref(),
                        );
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, ns_owned.as_deref()),
                        )?;
                        Ok((page_items, current_rv, pending_reserved_rv))
                    })
                    .await?
                }
            } else {
                // Cluster-scoped: base param count WITHOUT token/cursor.
                let base_param_count = 2 + if field_pushdown.sql_name_eq.is_some() {
                    1
                } else {
                    0
                };

                let index_pushdown = selector_index::build_selector_pushdown(
                    label_requirements.as_deref().unwrap_or(&[]),
                    &field_conditions_raw,
                    &av,
                    &k,
                    base_param_count,
                    true,
                );

                let has_residual = !index_pushdown.residual_labels.is_empty()
                    || !index_pushdown.residual_fields.is_empty();

                if has_residual {
                    let batch_size = (lim * selector_index::SELECTOR_RESIDUAL_SCAN_FACTOR)
                        .clamp(128, selector_index::SELECTOR_RESIDUAL_MAX_CANDIDATES);

                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let mut base_query = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
                             FROM cluster_resources r WHERE r.api_version = ?1 AND r.kind = ?2".to_string();
                        let mut base_param_strings: Vec<String> =
                            vec![av.clone(), k.clone()];
                        if let Some(name_eq) = field_pushdown.sql_name_eq.as_ref() {
                            base_query.push_str(&format!(
                                " AND r.name = ?{}",
                                base_param_strings.len() + 1
                            ));
                            base_param_strings.push(name_eq.clone());
                        }
                        for clause in &index_pushdown.sql_clauses {
                            base_query.push_str(&format!(" AND {clause}"));
                        }
                        for p in &index_pushdown.sql_params {
                            base_param_strings.push(p.clone());
                        }

                        let residual_labels = &index_pushdown.residual_labels;
                        let residual_fields = &index_pushdown.residual_fields;
                        let mut page_items = Vec::with_capacity(lim + 1);
                        let mut cursor_name = token_owned.clone();

                        loop {
                            let mut query = base_query.clone();
                            let mut param_strings = base_param_strings.clone();
                            if let Some(cursor) = &cursor_name {
                                query.push_str(&format!(
                                    " AND r.name > ?{}",
                                    param_strings.len() + 1
                                ));
                                param_strings.push(cursor.clone());
                            }
                            query.push_str(" ORDER BY r.name");
                            query.push_str(&format!(" LIMIT ?{}", param_strings.len() + 1));

                            let mut params: Vec<Box<dyn rusqlite::ToSql>> = param_strings
                                .iter()
                                .map(|s| Box::new(s.clone()) as Box<dyn rusqlite::ToSql>)
                                .collect();
                            params.push(Box::new(batch_size as i64));
                            let param_refs: Vec<&dyn rusqlite::ToSql> =
                                params.iter().map(|p| p.as_ref()).collect();

                            let mut stmt = conn.prepare(&query)?;
                            let rows = stmt.query_map(&param_refs[..], |row| {
                                let data_bytes: Vec<u8> = row.get(6)?;
                                let data: Value = serde_json::from_slice(&data_bytes)
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

                            let mut batch_count = 0usize;
                            let mut last_candidate_name: Option<String> = None;
                            for row in rows {
                                let item = row?;
                                batch_count += 1;
                                last_candidate_name = Some(item.name.clone());
                                if !residual_labels.is_empty()
                                    && !matches_label_requirements(
                                        &item.data,
                                        residual_labels,
                                    )
                                {
                                    continue;
                                }
                                if !residual_fields.is_empty()
                                    && !matches_field_selector_conditions(
                                        &item.data,
                                        residual_fields,
                                    )
                                {
                                    continue;
                                }
                                if page_items.len() <= lim {
                                    page_items.push(item);
                                } else {
                                    break;
                                }
                            }

                            if page_items.len() > lim {
                                break;
                            }
                            if batch_count < batch_size {
                                break;
                            }
                            cursor_name = last_candidate_name;
                        }
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, None),
                        )?;
                        Ok((page_items, current_rv, pending_reserved_rv))
                    })
                    .await?
                } else {
                    // Fully indexed: single query with limit+1.
                    let base_param_count_with_token =
                        base_param_count + if token_owned.is_some() { 1 } else { 0 };
                    let index_pushdown = selector_index::build_selector_pushdown(
                        label_requirements.as_deref().unwrap_or(&[]),
                        &field_conditions_raw,
                        &av,
                        &k,
                        base_param_count_with_token,
                        true,
                    );
                    self.db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let mut query = "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
                             FROM cluster_resources r WHERE r.api_version = ?1 AND r.kind = ?2".to_string();
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(av.clone()), Box::new(k.clone())];
                        if let Some(name_eq) = field_pushdown.sql_name_eq.as_ref() {
                            query.push_str(&format!(" AND r.name = ?{}", params.len() + 1));
                            params.push(Box::new(name_eq.clone()));
                        }
                        if let Some(token) = &token_owned {
                            query.push_str(&format!(" AND r.name > ?{}", params.len() + 1));
                            params.push(Box::new(token.clone()));
                        }
                        for clause in &index_pushdown.sql_clauses {
                            query.push_str(&format!(" AND {clause}"));
                        }
                        for p in &index_pushdown.sql_params {
                            params.push(Box::new(p.clone()));
                        }
                        query.push_str(" ORDER BY r.name");
                        query.push_str(&format!(" LIMIT ?{}", params.len() + 1));
                        params.push(Box::new((lim + 1) as i64));

                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let mut stmt = conn.prepare(&query)?;
                        let rows = stmt.query_map(&param_refs[..], |row| {
                            let data_bytes: Vec<u8> = row.get(6)?;
                            let data: Value = serde_json::from_slice(&data_bytes)
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
                        let mut page_items = Vec::with_capacity(lim);
                        for row in rows {
                            let item = row?;
                            if page_items.len() <= lim {
                                page_items.push(item);
                            } else {
                                break;
                            }
                        }
                        let current_rv = Self::current_resource_version_in_tx(&tx)?;
                        let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                            &tx,
                            &list_subject_key_prefix(&av, &k, None),
                        )?;
                        Ok((page_items, current_rv, pending_reserved_rv))
                    })
                    .await?
                }
            };

            // If we collected more than `lim` items, the extra one proves more
            // rows exist → pop it and set continue_token. Selector queries
            // always omit exact remainingItemCount.
            let mut items = items;
            let mut next_token = None;
            if items.len() > lim {
                items.truncate(lim);
                next_token = items.last().map(|r| r.name.clone());
            }
            let response_rv = list_response_resource_version(
                &items,
                current_rv,
                pending_reserved_rv,
                request_had_continue,
                next_token.is_some(),
            );

            return Ok(ResourceList {
                items,
                resource_version: response_rv,
                continue_token: next_token,
                remaining_item_count: None,
            });
        }

        // Route to correct table based on resource scope
        // Items are sorted by name for stable, alphabetical pagination.
        // Continue token is the name of the last item seen (exclusive lower bound).
        let event_compat_default = needs_event_v1_compat(api_version, kind);
        let has_selectors = label_selector.is_some() || field_selector.is_some();

        // Pre-parse selectors outside the db_call closure (they return
        // anyhow::Result which doesn't convert to rusqlite::Error).
        let no_limit_label_reqs = if has_selectors {
            if let Some(sel) = label_selector {
                Some(parse_label_selector(sel)?)
            } else {
                None
            }
        } else {
            None
        };
        let no_limit_field_pushdown = if has_selectors {
            field_selector
                .map(split_sql_pushdown_conditions)
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let no_limit_field_conditions =
            if has_selectors && !no_limit_field_pushdown.residual_selector.is_empty() {
                parse_field_selector_conditions(&no_limit_field_pushdown.residual_selector)
            } else {
                Vec::new()
            };

        let (items, current_rv, pending_reserved_rv) = if use_namespaced_table(
            api_version,
            kind,
            &namespace,
        ) {
            // Namespaced resources
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction()?;
                let conn = &tx;
                let (where_head, mut params): (&str, Vec<Box<dyn rusqlite::ToSql>>) =
                    if event_compat_default {
                        (
                            queries::NAMESPACED_LIST_BY_KIND_EVENT_COMPAT_HEAD,
                            vec![Box::new(k.clone())],
                        )
                    } else {
                        (
                            queries::NAMESPACED_LIST_BY_AV_KIND_HEAD,
                            vec![Box::new(av.clone()), Box::new(k.clone())],
                        )
                    };
                // Use table alias `r` when selectors need it for EXISTS subqueries.
                let mut query = if has_selectors {
                    format!(
                        "SELECT r.id, r.api_version, r.kind, r.namespace, r.name, r.resource_version, r.uid, r.data \
                         FROM namespaced_resources r {where_head}"
                    )
                } else {
                    format!("{}{where_head}", queries::NAMESPACED_LIST_HEAD,)
                };
                let col_prefix = if has_selectors { "r." } else { "" };
                if let Some(ref ns_val) = ns_owned {
                    query.push_str(&format!(" AND {col_prefix}namespace = ?{}", params.len() + 1));
                    params.push(Box::new(ns_val.clone()));
                }
                if let Some(name_eq) = no_limit_field_pushdown.sql_name_eq.as_ref() {
                    query.push_str(&format!(" AND {col_prefix}name = ?{}", params.len() + 1));
                    params.push(Box::new(name_eq.clone()));
                }
                if let Some(ns_eq) = no_limit_field_pushdown.sql_namespace_eq.as_ref() {
                    query.push_str(&format!(" AND {col_prefix}namespace = ?{}", params.len() + 1));
                    params.push(Box::new(ns_eq.clone()));
                }
                if let Some(token) = &token_owned {
                    query.push_str(&format!(" AND {col_prefix}name > ?{}", params.len() + 1));
                    params.push(Box::new(token.clone()));
                }
                // When selectors are present, push label/field index conditions
                // into SQL to reduce JSON decoding, even without a LIMIT clause.
                let mut index_residual_labels = Vec::new();
                let mut index_residual_fields = Vec::new();
                if has_selectors {
                    let label_reqs = no_limit_label_reqs.as_deref().unwrap_or(&[]);
                    let base_params = params.len();
                    let pd = selector_index::build_selector_pushdown(
                        label_reqs,
                        &no_limit_field_conditions,
                        &av,
                        &k,
                        base_params,
                        false,
                    );
                    for clause in &pd.sql_clauses {
                        query.push_str(&format!(" AND {clause}"));
                    }
                    for p in &pd.sql_params {
                        params.push(Box::new(p.clone()));
                    }
                    index_residual_labels = pd.residual_labels;
                    index_residual_fields = pd.residual_fields;
                }
                query.push_str(if has_selectors { " ORDER BY r.name" } else { " ORDER BY name" });
                let param_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(&param_refs[..], row_to_namespaced_resource)?;
                let mut items = Vec::new();
                for row in rows {
                    let item = row?;
                    if !index_residual_labels.is_empty()
                        && !matches_label_requirements(&item.data, &index_residual_labels)
                    {
                        continue;
                    }
                    if !index_residual_fields.is_empty()
                        && !matches_field_selector_conditions(&item.data, &index_residual_fields)
                    {
                        continue;
                    }
                    items.push(item);
                }
                let current_rv = Self::current_resource_version_in_tx(&tx)?;
                let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                    &tx,
                    &list_subject_key_prefix(&av, &k, ns_owned.as_deref()),
                )?;
                Ok((items, current_rv, pending_reserved_rv))
            })
            .await?
        } else {
            // Cluster-scoped resources
            self.db_call("db_query", move |conn| {
                let tx = conn.transaction()?;
                let conn = &tx;
                // CLUSTER_LIST_HEAD already uses unaliased columns; add `r.`
                // alias when selectors need it for EXISTS subqueries.
                let mut query = if has_selectors {
                    "SELECT r.id, r.api_version, r.kind, r.name, r.resource_version, r.uid, r.data \
                     FROM cluster_resources r WHERE r.api_version = ?1 AND r.kind = ?2"
                        .to_string()
                } else {
                    queries::CLUSTER_LIST_HEAD.to_string()
                };
                let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                    vec![Box::new(av.clone()), Box::new(k.clone())];
                let col_prefix = if has_selectors { "r." } else { "" };
                if let Some(name_eq) = no_limit_field_pushdown.sql_name_eq.as_ref() {
                    query.push_str(&format!(" AND {col_prefix}name = ?{}", params.len() + 1));
                    params.push(Box::new(name_eq.clone()));
                }
                if let Some(token) = &token_owned {
                    query.push_str(&format!(" AND {col_prefix}name > ?{}", params.len() + 1));
                    params.push(Box::new(token.clone()));
                }
                let mut index_residual_labels = Vec::new();
                let mut index_residual_fields = Vec::new();
                if has_selectors {
                    let label_reqs = no_limit_label_reqs.as_deref().unwrap_or(&[]);
                    let base_params = params.len();
                    let pd = selector_index::build_selector_pushdown(
                        label_reqs,
                        &no_limit_field_conditions,
                        &av,
                        &k,
                        base_params,
                        true,
                    );
                    for clause in &pd.sql_clauses {
                        query.push_str(&format!(" AND {clause}"));
                    }
                    for p in &pd.sql_params {
                        params.push(Box::new(p.clone()));
                    }
                    index_residual_labels = pd.residual_labels;
                    index_residual_fields = pd.residual_fields;
                }
                query.push_str(if has_selectors {
                    " ORDER BY r.name"
                } else {
                    " ORDER BY name"
                });
                let param_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(&param_refs[..], row_to_cluster_resource)?;
                let mut items = Vec::new();
                for row in rows {
                    let item = row?;
                    if !index_residual_labels.is_empty()
                        && !matches_label_requirements(&item.data, &index_residual_labels)
                    {
                        continue;
                    }
                    if !index_residual_fields.is_empty()
                        && !matches_field_selector_conditions(&item.data, &index_residual_fields)
                    {
                        continue;
                    }
                    items.push(item);
                }
                let current_rv = Self::current_resource_version_in_tx(&tx)?;
                let pending_reserved_rv = pending_reserved_rv_for_collection_in_tx(
                    &tx,
                    &list_subject_key_prefix(&av, &k, None),
                )?;
                Ok((items, current_rv, pending_reserved_rv))
            })
            .await?
        };
        // Label and field selector filtering is now handled via SQL pushdown
        // above, so the Rust-side filter_by_labels/filter_by_field_selector
        // calls are no longer needed.
        let mut items = items;
        let mut next_token: Option<String> = None;
        let mut remaining_item_count: Option<i64> = None;
        if let Some(lim) = limit
            && items.len() > lim as usize
        {
            // Accurate remaining_item_count: we fetched all items after the continue token,
            // so remaining = total_after_token - page_size.
            // K8s conformance requires remainingItemCount + len(items) == total.
            remaining_item_count = Some((items.len() - lim as usize) as i64);
            items.truncate(lim as usize);
            next_token = Some(items.last().unwrap().name.clone());
        }
        let response_rv = list_response_resource_version(
            &items,
            current_rv,
            pending_reserved_rv,
            request_had_continue,
            next_token.is_some(),
        );
        Ok(ResourceList {
            items,
            resource_version: response_rv,
            continue_token: next_token,
            remaining_item_count,
        })
    }

    pub async fn list_resources_page(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        page: ListPageRequest,
    ) -> Result<ResourceList> {
        self.list_resources(
            api_version,
            kind,
            namespace,
            ResourceListQuery::new(
                label_selector,
                field_selector,
                page.limit(),
                page.continue_token(),
            ),
        )
        .await
    }

    pub async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.db_call("list_cluster_resources", move |conn| {
            let mut stmt = conn.prepare(queries::CLUSTER_LIST_ALL)?;
            let rows = stmt.query_map([], row_to_cluster_resource)?;
            let mut items = Vec::new();
            for row in rows {
                items.push(row?);
            }
            Ok(items)
        })
        .await
        .map_err(|e| anyhow!("Failed to list cluster resources: {}", e))
    }

    /// List resource keys for a specific API version/kind from the chosen scope table.
    /// Used by CRD deletion cleanup to cascade delete custom resources.
    pub async fn list_resource_keys_for_scope(
        &self,
        api_version: String,
        kind: String,
        namespaced: bool,
    ) -> Result<Vec<(Option<String>, String)>> {
        if namespaced {
            self.db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::NAMESPACED_KEYS_FOR_SCOPE)?;
                let rows = stmt.query_map([api_version, kind], |row| {
                    Ok((Some(row.get::<_, String>(0)?), row.get::<_, String>(1)?))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            })
            .await
            .map_err(|e| anyhow!("Failed to list namespaced resource keys: {}", e))
        } else {
            self.db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::CLUSTER_KEYS_FOR_SCOPE)?;
                let rows = stmt.query_map([api_version, kind], |row| {
                    Ok((None, row.get::<_, String>(0)?))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            })
            .await
            .map_err(|e| anyhow!("Failed to list cluster resource keys: {}", e))
        }
    }

    pub async fn get_current_resource_version(&self) -> Result<i64> {
        let rv = self
            .db_call("db_query", |conn| {
                Ok(Self::current_resource_version_in_conn(conn)?)
            })
            .await?;
        Ok(rv)
    }

    /// Allocate a logical list snapshot resourceVersion without emitting a watch event.
    ///
    /// Kubernetes can return an inconsistent continuation after an expired token. That
    /// continuation starts a new list snapshot and must use a resourceVersion distinct
    /// from the original snapshot, even if no object changed while the token aged out.
    pub async fn advance_resource_version_after(&self, min_rv: i64) -> Result<i64> {
        let rv = self
            .db_call("db_query", move |conn| {
                Ok(Self::advance_resource_version_after_in_conn(conn, min_rv)?)
            })
            .await
            .map_err(|e| anyhow!("Failed to advance resource version: {}", e))?;
        Ok(rv)
    }
}
