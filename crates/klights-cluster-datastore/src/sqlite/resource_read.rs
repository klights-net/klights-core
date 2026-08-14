//! Resource read — get, list (with pagination/selectors), keys-for-scope,
//! cluster-resources, and resource-version helpers.

use anyhow::{Result, anyhow};
use klights_cluster_core::{Resource, WatchReplayPosition};
use serde_json::Value;

use super::filters::{
    matches_field_selector_conditions, matches_label_requirements, parse_label_selector,
    split_sql_pushdown_conditions,
};
use super::read_helpers::{
    needs_event_v1_compat, row_to_cluster_resource, row_to_namespaced_resource,
};
use super::read_queries as queries;
use super::read_store::{SqliteReadStore, SqliteResourceList, SqliteResourceListQuery};
use super::scope::use_namespaced_table;
use super::selector_index;

const MAX_INTERNAL_LIST_PREALLOCATION: usize = 4096;

#[cfg(any(test, feature = "test-support"))]
pub struct ListResourcesSnapshotPause {
    target: ListResourcesSnapshotPauseTarget,
    phase: ListResourcesSnapshotPausePhase,
    hit: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    blocking_resume: (std::sync::Mutex<bool>, std::sync::Condvar),
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone)]
pub struct ListResourcesSnapshotPauseTarget {
    api_version: String,
    kind: String,
    namespace: Option<String>,
    label_selector: Option<String>,
    field_selector: Option<String>,
    limit: Option<i64>,
    continue_token: Option<String>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum ListResourcesSnapshotPausePhase {
    BeforeQuery,
    AfterRows,
}

#[cfg(any(test, feature = "test-support"))]
static LIST_RESOURCES_SNAPSHOT_PAUSE: std::sync::OnceLock<
    std::sync::Mutex<Option<std::sync::Arc<ListResourcesSnapshotPause>>>,
> = std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-support"))]
impl ListResourcesSnapshotPause {
    pub async fn wait_for_hit(&self) {
        self.hit.notified().await;
    }

    pub fn resume(&self) {
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

#[cfg(any(test, feature = "test-support"))]
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

#[cfg(any(test, feature = "test-support"))]
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

fn list_response_resource_version(_items: &[Resource], current_rv: i64) -> i64 {
    // Every list (complete or paginated) reports the in-transaction global
    // snapshot resourceVersion. Because rows and `current_rv` are read inside
    // the same WAL read transaction, `current_rv` is already consistent with
    // the returned rows: a mutation that commits after this transaction's
    // snapshot (e.g. a concurrent delete) is invisible to it, so the snapshot
    // RV naturally precedes that mutation. This matches real K8s: the
    // collection revision anchors a follow-up `?watch=true&resourceVersion=<list
    // rv>` to "now", replaying nothing for objects already reflected in the
    // list (the kubectl `-w` phantom-pod artifact).
    current_rv
}

impl SqliteReadStore {
    #[cfg(any(test, feature = "test-support"))]
    pub fn resource_get_call_count_for_test(&self) -> u64 {
        self.resource_get_call_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn install_list_resources_snapshot_pause_for_test(
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

    #[cfg(any(test, feature = "test-support"))]
    pub fn install_list_resources_snapshot_after_rows_pause_for_test(
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
    #[cfg(any(test, feature = "test-support"))]
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
        #[cfg(any(test, feature = "test-support"))]
        self.resource_get_call_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

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
            self.read_db_call("db_query", move |conn| {
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
            self.read_db_call("db_query", move |conn| {
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
        query: SqliteResourceListQuery<'_>,
    ) -> Result<SqliteResourceList> {
        let SqliteResourceListQuery {
            label_selector,
            field_selector,
            limit,
            continue_token,
        } = query;
        if api_version == "v1" && kind == "Namespace" && namespace.is_none() {
            return self
                .list_namespaces_page(label_selector, field_selector, limit, continue_token)
                .await;
        }

        #[cfg(any(test, feature = "test-support"))]
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
        let selector_free_limited =
            label_selector.is_none() && field_selector.is_none() && limit.is_some();

        if selector_free_limited {
            let lim = limit.expect("selector_free_limited implies Some(limit)");
            let fetch_limit = lim;
            let event_compat = needs_event_v1_compat(api_version, kind);
            let (mut items, watch_position, total) = if use_namespaced_table(
                api_version,
                kind,
                &namespace,
            ) {
                self.read_db_call("db_query", move |conn| {
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
                    let mut count_query =
                        format!("SELECT COUNT(*) FROM namespaced_resources {where_head}");
                    if let Some(ref ns_val) = ns_owned {
                        let namespace_clause = format!(" AND namespace = ?{}", params.len() + 1);
                        query.push_str(&namespace_clause);
                        count_query.push_str(&namespace_clause);
                        params.push(Box::new(ns_val.clone()));
                    }
                    if let Some(token) = &token_owned {
                        let token_clause = format!(" AND name > ?{}", params.len() + 1);
                        query.push_str(&token_clause);
                        count_query.push_str(&token_clause);
                        params.push(Box::new(token.clone()));
                    }

                    let count_refs: Vec<&dyn rusqlite::ToSql> =
                        params.iter().map(|parameter| parameter.as_ref()).collect();
                    let total: i64 =
                        conn.query_row(&count_query, count_refs.as_slice(), |row| row.get(0))?;

                    query.push_str(&format!(" ORDER BY name LIMIT ?{}", params.len() + 1));
                    params.push(Box::new(fetch_limit));

                    let param_refs: Vec<&dyn rusqlite::ToSql> =
                        params.iter().map(|p| p.as_ref()).collect();
                    let mut stmt = conn.prepare(&query)?;
                    let rows = stmt.query_map(&param_refs[..], row_to_namespaced_resource)?;
                    // Bounded by `fetch_limit` (LIMIT clause); pre-size to avoid
                    // realloc churn on the common large-page list path.
                    let mut items = Vec::with_capacity(
                        usize::try_from(fetch_limit)
                            .unwrap_or(usize::MAX)
                            .min(MAX_INTERNAL_LIST_PREALLOCATION),
                    );
                    for row in rows {
                        items.push(row?);
                    }

                    let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                    Ok((items, watch_position, total))
                })
                .await?
            } else {
                self.read_db_call("db_query", move |conn| {
                        let tx = conn.transaction()?;
                        let conn = &tx;
                        let mut query = queries::CLUSTER_LIST_HEAD.to_string();
                        let mut count_query = String::from(
                            "SELECT COUNT(*) FROM cluster_resources WHERE api_version = ?1 AND kind = ?2",
                        );
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(av.clone()), Box::new(k.clone())];
                        if let Some(token) = &token_owned {
                            let token_clause = format!(" AND name > ?{}", params.len() + 1);
                            query.push_str(&token_clause);
                            count_query.push_str(&token_clause);
                            params.push(Box::new(token.clone()));
                        }
                        let count_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|parameter| parameter.as_ref()).collect();
                        let total: i64 =
                            conn.query_row(&count_query, count_refs.as_slice(), |row| row.get(0))?;
                        query.push_str(&format!(" ORDER BY name LIMIT ?{}", params.len() + 1));
                        params.push(Box::new(fetch_limit));

                        let param_refs: Vec<&dyn rusqlite::ToSql> =
                            params.iter().map(|p| p.as_ref()).collect();
                        let mut stmt = conn.prepare(&query)?;
                        let rows = stmt.query_map(&param_refs[..], row_to_cluster_resource)?;
                        // Bounded by `fetch_limit` (LIMIT clause); pre-size to avoid
                        // realloc churn on the common large-page list path.
                        let mut items = Vec::with_capacity(
                            usize::try_from(fetch_limit)
                                .unwrap_or(usize::MAX)
                                .min(MAX_INTERNAL_LIST_PREALLOCATION),
                        );
                        for row in rows {
                            items.push(row?);
                        }

                        let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                        Ok((items, watch_position, total))
                    })
                    .await?
            };

            let has_more = total > i64::try_from(items.len()).unwrap_or(i64::MAX);
            let page_limit = usize::try_from(lim).unwrap_or(usize::MAX);
            items.truncate(page_limit);
            let remaining_item_count = has_more
                .then(|| {
                    total
                        .checked_sub(i64::try_from(items.len()).unwrap_or(i64::MAX))
                        .ok_or_else(|| {
                            anyhow!("selector-free LIST remaining item count underflowed")
                        })
                })
                .transpose()?;
            let next_token = has_more
                .then(|| items.last().map(|r| r.name.clone()))
                .flatten();
            let response_rv =
                list_response_resource_version(&items, watch_position.resource_version);
            let watch_replay_position = Some(WatchReplayPosition {
                resource_version: response_rv,
                ..watch_position
            });

            return Ok(SqliteResourceList {
                items,
                resource_version: response_rv,
                watch_replay_position,
                continue_token: next_token,
                remaining_item_count,
            });
        }

        let selector_limited =
            limit.is_some() && (label_selector.is_some() || field_selector.is_some());
        if selector_limited {
            let lim = usize::try_from(limit.expect("selector_limited implies Some(limit)"))
                .unwrap_or(usize::MAX);
            let label_requirements = if let Some(selector) = label_selector {
                Some(parse_label_selector(selector)?)
            } else {
                None
            };

            // Split field selector into SQL-pushable (metadata.name/namespace)
            // and residual conditions.
            let field_pushdown = field_selector
                .map(split_sql_pushdown_conditions)
                .transpose()?
                .unwrap_or_default();
            let field_conditions_raw = field_pushdown.residual_fields;

            let event_compat_selector = needs_event_v1_compat(api_version, kind);

            // Build the pushdown separately for each branch because the param
            // offset (base query parameter count) differs between namespaced
            // and cluster paths.
            let (items, watch_position) = if use_namespaced_table(api_version, kind, &namespace) {
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

                    self.read_db_call("db_query", move |conn| {
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
                        let mut page_items = Vec::with_capacity(lim.saturating_add(1).min(MAX_INTERNAL_LIST_PREALLOCATION));
                        let mut cursor_name = token_owned.clone();

                        loop {
                            let (query, param_strings) = residual_selector_window_query(
                                &base_query,
                                &base_param_strings,
                                cursor_name.as_deref(),
                            );

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
                                        &item,
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
                        let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                        Ok((page_items, watch_position))
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
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_av = av.clone();
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_k = k.clone();
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_ns = ns_owned.clone();
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_label_selector = label_selector.map(str::to_string);
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_field_selector = field_selector.map(str::to_string);
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_limit = limit;
                    #[cfg(any(test, feature = "test-support"))]
                    let pause_continue_token = token_owned.clone();
                    self.read_db_call("db_query", move |conn| {
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
                        params.push(Box::new(i64::try_from(lim.saturating_add(1)).unwrap_or(i64::MAX)));

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
                        let mut page_items = Vec::with_capacity(lim.min(MAX_INTERNAL_LIST_PREALLOCATION));
                        for row in rows {
                            let item = row?;
                            if page_items.len() <= lim {
                                page_items.push(item);
                            } else {
                                break;
                            }
                        }
                        #[cfg(any(test, feature = "test-support"))]
                        maybe_pause_list_resources_snapshot_after_rows_for_test(
                            &pause_av,
                            &pause_k,
                            pause_ns.as_deref(),
                            pause_label_selector.as_deref(),
                            pause_field_selector.as_deref(),
                            pause_limit,
                            pause_continue_token.as_deref(),
                        );
                        let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                        Ok((page_items, watch_position))
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

                    self.read_db_call("db_query", move |conn| {
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
                        let mut page_items = Vec::with_capacity(lim.saturating_add(1).min(MAX_INTERNAL_LIST_PREALLOCATION));
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
                                        &item,
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
                        let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                        Ok((page_items, watch_position))
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
                    self.read_db_call("db_query", move |conn| {
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
                        params.push(Box::new(i64::try_from(lim.saturating_add(1)).unwrap_or(i64::MAX)));

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
                        let mut page_items = Vec::with_capacity(lim.min(MAX_INTERNAL_LIST_PREALLOCATION));
                        for row in rows {
                            let item = row?;
                            if page_items.len() <= lim {
                                page_items.push(item);
                            } else {
                                break;
                            }
                        }
                        let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                        Ok((page_items, watch_position))
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
            let response_rv =
                list_response_resource_version(&items, watch_position.resource_version);
            let watch_replay_position = Some(WatchReplayPosition {
                resource_version: response_rv,
                ..watch_position
            });

            return Ok(SqliteResourceList {
                items,
                resource_version: response_rv,
                watch_replay_position,
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
                .transpose()?
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let no_limit_field_conditions = no_limit_field_pushdown.residual_fields;

        let (items, watch_position) = if use_namespaced_table(api_version, kind, &namespace) {
            // Namespaced resources
            self.read_db_call("db_query", move |conn| {
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
                        && !matches_field_selector_conditions(&item, &index_residual_fields)
                    {
                        continue;
                    }
                    items.push(item);
                }
                let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                Ok((items, watch_position))
            })
            .await?
        } else {
            // Cluster-scoped resources
            self.read_db_call("db_query", move |conn| {
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
                        && !matches_field_selector_conditions(&item, &index_residual_fields)
                    {
                        continue;
                    }
                    items.push(item);
                }
                let watch_position = Self::current_watch_replay_position_in_tx(&tx)?;
                Ok((items, watch_position))
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
            && i64::try_from(items.len()).unwrap_or(i64::MAX) > lim
            && let Ok(lim) = usize::try_from(lim)
        {
            // Accurate remaining_item_count: we fetched all items after the continue token,
            // so remaining = total_after_token - page_size.
            // K8s conformance requires remainingItemCount + len(items) == total.
            remaining_item_count = Some(i64::try_from(items.len() - lim).unwrap_or(i64::MAX));
            items.truncate(lim);
            next_token = Some(items.last().unwrap().name.clone());
        }
        let response_rv = list_response_resource_version(&items, watch_position.resource_version);
        Ok(SqliteResourceList {
            items,
            resource_version: response_rv,
            watch_replay_position: Some(WatchReplayPosition {
                resource_version: response_rv,
                ..watch_position
            }),
            continue_token: next_token,
            remaining_item_count,
        })
    }

    pub async fn list_resources_for_watch_targets(
        &self,
        targets: &[klights_cluster_store::DurableWatchTarget],
        label_selector: Option<&str>,
    ) -> Result<klights_cluster_store::ResourceScopeSnapshot> {
        let targets = targets.to_vec();
        let label_requirements = label_selector
            .map(parse_label_selector)
            .transpose()?
            .unwrap_or_default();
        #[cfg(any(test, feature = "test-support"))]
        let label_selector_for_pause = label_selector.map(str::to_string);

        self.read_db_call("list_resources_for_watch_targets", move |conn| {
            let tx = conn.transaction()?;
            let mut items = Vec::new();
            for target in &targets {
                match target.scope() {
                    klights_cluster_store::DurableWatchScope::Cluster => {
                        let mut query = "SELECT r.id, r.api_version, r.kind, r.name, \
                             r.resource_version, r.uid, r.data \
                             FROM cluster_resources r \
                             WHERE r.api_version = ?1 AND r.kind = ?2"
                            .to_string();
                        let api_version = target.api_version().to_string();
                        let kind = target.kind().to_string();
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(api_version.clone()), Box::new(kind.clone())];
                        let pushdown = selector_index::build_selector_pushdown(
                            &label_requirements,
                            &[],
                            &api_version,
                            &kind,
                            params.len(),
                            true,
                        );
                        for clause in &pushdown.sql_clauses {
                            query.push_str(&format!(" AND {clause}"));
                        }
                        for parameter in pushdown.sql_params {
                            params.push(Box::new(parameter));
                        }
                        let residual_labels = pushdown.residual_labels;
                        query.push_str(" ORDER BY r.name");
                        let param_refs = params
                            .iter()
                            .map(|parameter| parameter.as_ref())
                            .collect::<Vec<&dyn rusqlite::ToSql>>();
                        let mut stmt = tx.prepare(&query)?;
                        let rows =
                            stmt.query_map(param_refs.as_slice(), row_to_cluster_resource)?;
                        for row in rows {
                            let item = row?;
                            if residual_labels.is_empty()
                                || matches_label_requirements(&item.data, &residual_labels)
                            {
                                items.push(item);
                            }
                        }
                    }
                    klights_cluster_store::DurableWatchScope::Namespaced(namespace) => {
                        let mut query = "SELECT r.id, r.api_version, r.kind, r.namespace, \
                             r.name, r.resource_version, r.uid, r.data \
                             FROM namespaced_resources r \
                             WHERE r.api_version = ?1 AND r.kind = ?2"
                            .to_string();
                        let api_version = target.api_version().to_string();
                        let kind = target.kind().to_string();
                        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                            vec![Box::new(api_version.clone()), Box::new(kind.clone())];
                        if let Some(namespace) = namespace {
                            query.push_str(" AND r.namespace = ?3");
                            params.push(Box::new(namespace.clone()));
                        }
                        let pushdown = selector_index::build_selector_pushdown(
                            &label_requirements,
                            &[],
                            &api_version,
                            &kind,
                            params.len(),
                            false,
                        );
                        for clause in &pushdown.sql_clauses {
                            query.push_str(&format!(" AND {clause}"));
                        }
                        for parameter in pushdown.sql_params {
                            params.push(Box::new(parameter));
                        }
                        let residual_labels = pushdown.residual_labels;
                        query.push_str(" ORDER BY r.namespace, r.name");
                        let param_refs = params
                            .iter()
                            .map(|param| param.as_ref())
                            .collect::<Vec<&dyn rusqlite::ToSql>>();
                        let mut stmt = tx.prepare(&query)?;
                        let rows =
                            stmt.query_map(param_refs.as_slice(), row_to_namespaced_resource)?;
                        for row in rows {
                            let item = row?;
                            if residual_labels.is_empty()
                                || matches_label_requirements(&item.data, &residual_labels)
                            {
                                items.push(item);
                            }
                        }
                    }
                }
                #[cfg(any(test, feature = "test-support"))]
                maybe_pause_list_resources_snapshot_after_rows_for_test(
                    target.api_version(),
                    target.kind(),
                    match target.scope() {
                        klights_cluster_store::DurableWatchScope::Cluster => None,
                        klights_cluster_store::DurableWatchScope::Namespaced(namespace) => {
                            namespace.as_deref()
                        }
                    },
                    label_selector_for_pause.as_deref(),
                    None,
                    None,
                    None,
                );
            }

            let mut watch_replay_position = Self::current_watch_replay_position_in_tx(&tx)?;
            let response_rv =
                list_response_resource_version(&items, watch_replay_position.resource_version);
            watch_replay_position.resource_version = response_rv;
            klights_cluster_store::ResourceScopeSnapshot::try_new(items, watch_replay_position)
                .map_err(|error| {
                    klights_supervisor::DbError::Application(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    )))
                })
        })
        .await
        .map_err(|err| anyhow!("Failed to atomically list watch targets: {err}"))
    }

    pub async fn list_cluster_resources(&self) -> Result<Vec<Resource>> {
        self.read_db_call("list_cluster_resources", move |conn| {
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
    ) -> Result<Vec<klights_cluster_store::ResourceCollectionKey>> {
        if namespaced {
            self.read_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::NAMESPACED_KEYS_FOR_SCOPE)?;
                let rows = stmt.query_map([api_version, kind], |row| {
                    Ok(klights_cluster_store::ResourceCollectionKey::new(
                        Some(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                    ))
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
            self.read_db_call("db_query", move |conn| {
                let mut stmt = conn.prepare(queries::CLUSTER_KEYS_FOR_SCOPE)?;
                let rows = stmt.query_map([api_version, kind], |row| {
                    Ok(klights_cluster_store::ResourceCollectionKey::new(
                        None::<String>,
                        row.get::<_, String>(0)?,
                    ))
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
            .read_db_call("db_query", |conn| {
                Ok(Self::current_resource_version_in_conn(conn)?)
            })
            .await?;
        Ok(rv)
    }
}

/// Builds exactly one residual-selector candidate window.  The returned SQL
/// has a single ordered, exclusive scan cursor and a final bound LIMIT; the
/// async extraction uses this value to issue one DB closure per window.
fn residual_selector_window_query(
    base_query: &str,
    base_params: &[String],
    cursor_name: Option<&str>,
) -> (String, Vec<String>) {
    let mut query = base_query.to_string();
    let mut params = base_params.to_vec();
    if let Some(cursor) = cursor_name {
        query.push_str(&format!(" AND r.name > ?{}", params.len() + 1));
        params.push(cursor.to_string());
    }
    query.push_str(" ORDER BY r.name");
    query.push_str(&format!(" LIMIT ?{}", params.len() + 1));
    (query, params)
}
