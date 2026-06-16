use super::queries;
use super::*;
use anyhow::Result;

impl Datastore {
    pub async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();

        let items = self
            .db_call("list_cluster_resources_modified_since", move |conn| {
                let mut stmt = conn.prepare(queries::WATCH_EVENTS_LIST_CLUSTER_SINCE)?;
                let rows = stmt.query_map(
                    rusqlite::params![api_version, kind, since_rv],
                    Self::watch_row_to_catchup_resource,
                )?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(row?);
                }
                Ok(items)
            })
            .await?;

        Ok(items)
    }

    /// List namespaced watch events of a given kind after `since_rv`
    /// (resource_version > since_rv), ordered by resource_version ascending.
    pub async fn list_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace_owned = namespace.map(str::to_string);

        let items = self
            .db_call("list_resources_modified_since", move |conn| {
                let mut query = queries::WATCH_EVENTS_LIST_NAMESPACED_SINCE_HEAD.to_string();
                let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                    vec![Box::new(api_version), Box::new(kind), Box::new(since_rv)];

                if let Some(ref ns) = namespace_owned {
                    query.push_str(&format!(" AND namespace = ?{}", params.len() + 1));
                    params.push(Box::new(ns.clone()));
                }

                query.push_str(" ORDER BY resource_version ASC");

                let param_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(&param_refs[..], Self::watch_row_to_catchup_resource)?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(row?);
                }
                Ok(items)
            })
            .await?;

        Ok(items)
    }

    /// Total `watch_events` rows currently held. Used by GC tests and could
    /// be surfaced as an ops metric in the future.
    #[cfg(test)]
    pub async fn count_watch_events(&self) -> Result<i64> {
        let count = self
            .db_call("count_watch_events", |conn| {
                Ok(conn.query_row::<i64, _, _>(queries::WATCH_EVENTS_COUNT, [], |r| r.get(0))?)
            })
            .await
            .map_err(|e| anyhow!("Failed to count watch_events: {}", e))?;
        Ok(count)
    }

    /// Garbage-collect old `watch_events` rows so the table holds a bounded
    /// sliding window of the most recent events. Returns the number of rows
    /// deleted. The id-based bound is O(1) — no `COUNT(*)` scan.
    ///
    /// Workers that fall behind this window get `RecvError::Lagged` → replay
    /// via `DatastoreWatchReplaySource`; workers further behind than the
    /// persisted window get `410 Gone` and relist.
    pub async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> Result<usize> {
        let count = self
            .db_call("watch_events_gc_prunable_count", move |conn| {
                Ok(conn.query_row::<i64, _, _>(
                    queries::WATCH_EVENTS_GC_PRUNABLE_COUNT,
                    rusqlite::params![max_rows, batch_cap],
                    |row| row.get(0),
                )? as usize)
            })
            .await
            .map_err(|e| anyhow!("Failed to count prunable watch_events: {}", e))?;
        Ok(count)
    }

    pub async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        let deleted = self
            .db_call("gc_watch_events", move |conn| {
                let removed = conn.execute(
                    queries::WATCH_EVENTS_GC,
                    rusqlite::params![max_rows, batch_cap],
                )?;
                // After deleting rows, release freed pages back to the OS if
                // at least one page worth of rows was removed.
                if removed > 0 {
                    let _ = conn.execute("PRAGMA incremental_vacuum(1000)", []);
                }
                Ok(removed)
            })
            .await
            .map_err(|e| anyhow!("Failed to gc watch_events: {}", e))?;
        Ok(deleted)
    }

    /// Lowest `resource_version` still retained in `watch_events`, or `None`
    /// when the table is empty. A watch resuming from an RV older than this
    /// has fallen outside the replay window and must be answered with
    /// `410 Gone` so the client reflector relists.
    pub async fn earliest_watch_event_rv(&self) -> Result<Option<i64>> {
        let rv = self
            .db_call("earliest_watch_event_rv", move |conn| {
                let mut stmt = conn.prepare(queries::WATCH_EVENTS_MIN_RV)?;
                let mut rows = stmt.query([])?;
                match rows.next()? {
                    Some(row) => Ok(Some(row.get::<_, i64>(0)?)),
                    None => Ok(None),
                }
            })
            .await
            .map_err(|e| anyhow!("Failed to read earliest watch_event rv: {}", e))?;
        Ok(rv)
    }

    pub async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        let targets = targets.to_vec();
        let items = self
            .db_call("list_watch_events_since", move |conn| {
                let mut query = queries::WATCH_EVENTS_LIST_TARGETS_HEAD.to_string();
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(since_rv)];

                for (idx, target) in targets.iter().enumerate() {
                    if idx > 0 {
                        query.push_str(" OR ");
                    }
                    query.push('(');
                    query.push_str(&format!(
                        "api_version = ?{} AND kind = ?{}",
                        params.len() + 1,
                        params.len() + 2
                    ));
                    params.push(Box::new(target.api_version.clone()));
                    params.push(Box::new(target.kind.clone()));

                    match &target.scope {
                        WatchTargetScope::Cluster => {
                            query.push_str(" AND namespace IS NULL");
                        }
                        WatchTargetScope::Namespaced(Some(namespace)) => {
                            query.push_str(&format!(" AND namespace = ?{}", params.len() + 1));
                            params.push(Box::new(namespace.clone()));
                        }
                        WatchTargetScope::Namespaced(None) => {
                            query.push_str(" AND namespace IS NOT NULL");
                        }
                    }
                    query.push(')');
                }

                query.push_str(") ORDER BY resource_version ASC");

                let param_refs: Vec<&dyn rusqlite::ToSql> =
                    params.iter().map(|param| param.as_ref()).collect();
                let mut stmt = conn.prepare(&query)?;
                let rows = stmt.query_map(&param_refs[..], Self::watch_row_to_catchup_resource)?;
                let mut items = Vec::new();
                for row in rows {
                    items.push(row?);
                }
                Ok(items)
            })
            .await?;

        Ok(items)
    }

    pub async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        let items = self
            .db_call("list_all_watch_events_since", move |conn| {
                let mut stmt = conn.prepare(queries::WATCH_EVENTS_LIST_ALL_SINCE)?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![since_rv],
                        Self::watch_row_to_catchup_resource,
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;

        Ok(items)
    }

    pub async fn list_deleted_watch_events_since(
        &self,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        let items = self
            .db_call("list_deleted_watch_events_since", move |conn| {
                let mut stmt = conn.prepare(queries::WATCH_EVENTS_LIST_DELETED_SINCE)?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![since_rv],
                        Self::watch_row_to_catchup_resource,
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;

        Ok(items)
    }
}
