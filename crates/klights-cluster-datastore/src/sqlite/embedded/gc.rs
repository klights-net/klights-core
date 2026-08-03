#[cfg(test)]
use super::live_apply::watch_events_min_scope_rows_for_scope_count;
use super::live_apply::{gc_watch_events_in_tx, watch_events_min_scope_rows_in_conn};
use super::queries;
use super::*;
use anyhow::Result;
use klights_cluster_core::{PositionedWatchEvent, WatchReplayPosition};
use klights_cluster_store::{PositionedWatchReplay, PositionedWatchReplayRead, WatchReplayRead};

impl Datastore {
    pub async fn list_cluster_resources_modified_since(
        &self,
        api_version: &str,
        kind: &str,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::ModifiedClusterResourcesRequest::try_new(
            api_version,
            kind,
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_reads
            .list_cluster_resources_modified_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
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
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::ModifiedResourcesRequest::try_new(
            api_version,
            kind,
            namespace.map(str::to_string),
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_reads
            .list_resources_modified_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    /// Total `watch_events` rows currently held. Used by GC tests and could
    /// be surfaced as an ops metric in the future.
    #[cfg(any(test, feature = "test-support"))]
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
    /// deleted. The global id bound keeps high-churn scopes compact, while the
    /// per-resource-scope floor prevents unrelated churn from deleting rare
    /// resource history needed by lagged watches.
    ///
    /// Workers that fall behind this window get `RecvError::Lagged` → replay
    /// through the positioned watch-history port; workers further behind than the
    /// persisted window get `410 Gone` and relist.
    pub async fn watch_events_gc_prunable_count(
        &self,
        max_rows: i64,
        batch_cap: i64,
    ) -> Result<usize> {
        let count = self
            .db_call("watch_events_gc_prunable_count", move |conn| {
                let min_scope_rows = watch_events_min_scope_rows_in_conn(conn, max_rows)?;
                Ok(conn.query_row::<i64, _, _>(
                    queries::WATCH_EVENTS_GC_PRUNABLE_COUNT,
                    rusqlite::params![max_rows, batch_cap, min_scope_rows],
                    |row| row.get(0),
                )? as usize)
            })
            .await
            .map_err(|e| anyhow!("Failed to count prunable watch_events: {}", e))?;
        Ok(count)
    }

    /// Count how many applied_outbox rows are eligible for GC at the provided
    /// cutoff without mutating storage.
    pub async fn applied_outbox_gc_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        let count = self
            .db_call("applied_outbox_gc_prunable_count", move |conn| {
                Ok(conn.query_row::<i64, _, _>(
                    queries::APPLIED_OUTBOX_GC_PRUNABLE_COUNT,
                    rusqlite::params![cutoff_ms],
                    |row| row.get(0),
                )? as usize)
            })
            .await
            .map_err(|e| anyhow!("Failed to count prunable applied_outbox rows: {}", e))?;
        Ok(count)
    }

    pub async fn gc_watch_events(&self, max_rows: i64, batch_cap: i64) -> Result<usize> {
        let deleted = self
            .db_call("gc_watch_events", move |conn| {
                let tx = conn.transaction()?;
                let removed = gc_watch_events_in_tx(&tx, max_rows, batch_cap)?;
                tx.commit()?;

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
        use klights_cluster_store::DurableWatchRangeRead as _;

        self.focused_reads
            .earliest_watch_event_rv()
            .await
            .map_err(anyhow::Error::new)
    }

    pub async fn list_watch_events_since(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchEventsSinceRequest::try_new(
            focused_watch_targets(targets),
            since_rv,
        )
        .map_err(anyhow::Error::new)?;
        self.focused_reads
            .list_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    /// Atomically check the retained watch history floor and read a replay
    /// suffix. Keeping both operations inside one SQLite connection call gives
    /// this closure a single read snapshot, so watch_events GC cannot advance
    /// the floor between "safe to replay" and the replay query.
    pub async fn list_watch_events_since_checked(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
    ) -> Result<WatchReplayRead> {
        match self
            .focused_reads
            .replay_watch_events_since_checked(&focused_watch_targets(targets), since_rv, None)
            .await
            .map_err(anyhow::Error::new)?
        {
            crate::sqlite::read_store::SqliteCheckedWatchRead::Expired => {
                Ok(WatchReplayRead::Expired)
            }
            crate::sqlite::read_store::SqliteCheckedWatchRead::Events(events) => {
                Ok(WatchReplayRead::Events(focused_events_to_catchup(events)))
            }
        }
    }

    pub async fn list_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead> {
        match self
            .focused_reads
            .replay_watch_events_since_checked(
                &focused_watch_targets(targets),
                since_rv,
                Some(limit),
            )
            .await
            .map_err(anyhow::Error::new)?
        {
            crate::sqlite::read_store::SqliteCheckedWatchRead::Expired => {
                Ok(WatchReplayRead::Expired)
            }
            crate::sqlite::read_store::SqliteCheckedWatchRead::Events(events) => {
                Ok(WatchReplayRead::Events(focused_events_to_catchup(events)))
            }
        }
    }

    pub async fn list_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<CatchUpResource>> {
        use klights_cluster_store::DurableWatchHistoryRead as _;

        let request = klights_cluster_store::WatchHistoryRequest::new(
            focused_watch_targets(targets),
            position,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_reads
            .replay_watch_history(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::WatchHistoryRead::Expired => {
                Ok(PositionedWatchReplayRead::Expired)
            }
            klights_cluster_store::WatchHistoryRead::Events(page) => {
                let next_position = page.next_position();
                let events = page
                    .into_events()
                    .into_iter()
                    .map(|event| {
                        let event_type = event.event.event_type().to_string();
                        PositionedWatchEvent {
                            position: event.position,
                            event: CatchUpResource {
                                resource: event.event.into_resource(),
                                event_type: std::borrow::Cow::Owned(event_type),
                            },
                        }
                    })
                    .collect();
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events,
                    next_position,
                }))
            }
        }
    }

    pub async fn current_watch_replay_position(&self) -> Result<WatchReplayPosition> {
        self.focused_reads
            .read_allocator_state()
            .await
            .map(|state| state.position())
            .map_err(anyhow::Error::new)
    }

    pub async fn list_raw_watch_events_since_checked_bounded(
        &self,
        targets: &[WatchTarget],
        since_rv: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<WatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        use klights_cluster_store::DurableRawWatchHistoryRead as _;

        let request = klights_cluster_store::RawWatchEventsSinceRequest::try_new(
            focused_watch_targets(targets),
            since_rv,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_reads
            .list_raw_watch_events_since_checked_bounded(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::RawWatchHistoryRead::Expired => Ok(WatchReplayRead::Expired),
            klights_cluster_store::RawWatchHistoryRead::Events(page) => {
                Ok(WatchReplayRead::Events(page.into_events()))
            }
        }
    }

    pub async fn list_raw_watch_events_after_position_checked_bounded(
        &self,
        targets: &[WatchTarget],
        position: WatchReplayPosition,
        limit: std::num::NonZeroUsize,
    ) -> Result<PositionedWatchReplayRead<klights_cluster_store::DurableRawWatchEvent>> {
        use klights_cluster_store::DurableRawWatchHistoryRead as _;

        let request = klights_cluster_store::RawWatchEventsAfterPositionRequest::try_new(
            focused_watch_targets(targets),
            position,
            limit.get(),
        )
        .map_err(anyhow::Error::new)?;
        match self
            .focused_reads
            .list_raw_watch_events_after_position_checked_bounded(request)
            .await
            .map_err(anyhow::Error::new)?
        {
            klights_cluster_store::PositionedRawWatchHistoryRead::Expired => {
                Ok(PositionedWatchReplayRead::Expired)
            }
            klights_cluster_store::PositionedRawWatchHistoryRead::Events(page) => {
                let next_position = page.next_position();
                Ok(PositionedWatchReplayRead::Events(PositionedWatchReplay {
                    events: page.into_events(),
                    next_position,
                }))
            }
        }
    }

    pub async fn list_all_watch_events_since(&self, since_rv: i64) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchRangeStart::try_new(since_rv)
            .map_err(anyhow::Error::new)?;
        self.focused_reads
            .list_all_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }

    /// memory-improvement.md §10 P1: keyset-paginated form of
    /// `list_all_watch_events_since`. Returns up to `limit` rows whose
    /// `(resource_version, id)` strictly follows `(<after_resource_version>,
    /// <after_id>)`, with `resource_version > since_rv`, in the same
    /// `(resource_version ASC, id ASC)` ordering as the full-list form.
    /// Each item carries its `watch_events.id` so the caller can advance the
    /// cursor. The first page passes `(since_rv, 0)`.
    pub async fn list_all_watch_events_since_paged(
        &self,
        since_rv: i64,
        after_resource_version: i64,
        after_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        let limit_i64 = limit.get() as i64;
        let items = self
            .db_call("list_all_watch_events_since_paged", move |conn| {
                let mut stmt = conn.prepare(queries::WATCH_EVENTS_LIST_ALL_SINCE_PAGED)?;
                let rows = stmt
                    .query_map(
                        rusqlite::params![since_rv, after_resource_version, after_id, limit_i64],
                        Self::watch_row_to_catchup_resource_with_id,
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await?;

        Ok(items)
    }

    pub async fn list_all_watch_events_after_id_bounded(
        &self,
        after_id: i64,
        through_id: i64,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<(i64, CatchUpResource)>> {
        let limit = limit.get() as i64;
        Ok(self
            .read_db_call("list_all_watch_events_after_id_bounded", move |conn| {
                let mut stmt = conn.prepare(
                "SELECT api_version, kind, namespace, name, resource_version, event_type, data, id
                 FROM watch_events
                 WHERE id > ?1 AND id <= ?2
                 ORDER BY id ASC
                 LIMIT ?3",
            )?;
                let rows = stmt.query_map(
                    rusqlite::params![after_id, through_id, limit],
                    Self::watch_row_to_catchup_resource_with_id,
                )?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await?)
    }

    pub async fn list_watch_replay_floors(
        &self,
    ) -> Result<Vec<klights_cluster_store::WatchReplayFloor>> {
        use klights_cluster_store::DurableWatchHistoryRead as _;

        self.focused_reads
            .list_replay_floors()
            .await
            .map(|floors| {
                floors
                    .into_iter()
                    .map(focused_replay_floor_to_legacy)
                    .collect()
            })
            .map_err(anyhow::Error::new)
    }

    pub async fn list_watch_replay_floors_paged(
        &self,
        after: Option<&klights_cluster_store::SnapshotReplayFloorCursor>,
        limit: std::num::NonZeroUsize,
    ) -> Result<Vec<klights_cluster_store::WatchReplayFloor>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow::anyhow!(
                "watch replay-floor page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after.map(|cursor| match cursor.target() {
            klights_cluster_store::DurableReplayTarget::All => {
                ("*".to_string(), "*".to_string(), "*".to_string())
            }
            klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
                (api_version.clone(), kind.clone(), "#cluster".to_string())
            }
            klights_cluster_store::DurableReplayTarget::Namespaced {
                api_version,
                kind,
                namespace,
            } => (api_version.clone(), kind.clone(), namespace.clone()),
        });
        let limit = i64::try_from(limit.get())?;
        Ok(self
            .read_db_call("list_watch_replay_floors_paged", move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT api_version, kind, namespace_key, floor_rv, floor_event_id,
                         floor_position_exact
                     FROM watch_replay_floors
                     WHERE ?1 IS NULL
                        OR api_version > ?1
                        OR (api_version = ?1 AND kind > ?2)
                        OR (api_version = ?1 AND kind = ?2 AND namespace_key > ?3)
                     ORDER BY api_version, kind, namespace_key
                     LIMIT ?4",
                )?;
                let rows = stmt.query_map(
                    rusqlite::params![
                        after.as_ref().map(|cursor| cursor.0.as_str()),
                        after.as_ref().map(|cursor| cursor.1.as_str()),
                        after.as_ref().map(|cursor| cursor.2.as_str()),
                        limit,
                    ],
                    |row| {
                        Ok(klights_cluster_store::WatchReplayFloor {
                            api_version: row.get(0)?,
                            kind: row.get(1)?,
                            namespace_key: row.get(2)?,
                            floor_resource_version: row.get(3)?,
                            floor_event_id: row.get(4)?,
                            position_is_exact: row.get(5)?,
                        })
                    },
                )?;
                Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
            })
            .await?)
    }

    pub async fn list_deleted_watch_events_since(
        &self,
        since_rv: i64,
    ) -> Result<Vec<CatchUpResource>> {
        use klights_cluster_store::DurableWatchRangeRead as _;

        let request = klights_cluster_store::WatchRangeStart::try_new(since_rv)
            .map_err(anyhow::Error::new)?;
        self.focused_reads
            .list_deleted_watch_events_since(request)
            .await
            .map(focused_events_to_catchup)
            .map_err(anyhow::Error::new)
    }
}

fn focused_replay_floor_to_legacy(
    floor: klights_cluster_store::DurableReplayFloor,
) -> klights_cluster_store::WatchReplayFloor {
    let (target, floor_resource_version, floor_event_id, position_is_exact) = floor.into_parts();
    let (api_version, kind, namespace_key) = match target {
        klights_cluster_store::DurableReplayTarget::All => {
            ("*".to_string(), "*".to_string(), "*".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Cluster { api_version, kind } => {
            (api_version, kind, "#cluster".to_string())
        }
        klights_cluster_store::DurableReplayTarget::Namespaced {
            api_version,
            kind,
            namespace,
        } => (api_version, kind, namespace),
    };
    klights_cluster_store::WatchReplayFloor {
        api_version,
        kind,
        namespace_key,
        floor_resource_version,
        floor_event_id,
        position_is_exact,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gc_watch_respects_batch_cap() {
        let db = Datastore::new_in_memory().await.unwrap();
        for i in 0..200 {
            db.create_resource(
                "v1",
                "ConfigMap",
                Some("default"),
                &format!("batch-cap-{i}"),
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": {"namespace": "default", "name": format!("batch-cap-{i}")},
                    "data": {"key": "value"}
                }),
            )
            .await
            .unwrap();
        }

        let removed = db.gc_watch_events(50, 30).await.unwrap();

        assert_eq!(
            removed, 30,
            "one retention mutation must honor its batch cap"
        );
    }

    #[test]
    fn watch_events_scope_floor_shrinks_when_scope_count_would_exceed_global_cap() {
        assert_eq!(
            watch_events_min_scope_rows_for_scope_count(100_000, 40),
            1_024,
            "normal scope counts keep the configured rare-scope floor"
        );
        assert_eq!(
            watch_events_min_scope_rows_for_scope_count(100_000, 1_000),
            100,
            "many e2e-created scopes must not reserve 1024 rows each"
        );
        assert_eq!(
            watch_events_min_scope_rows_for_scope_count(10, 30),
            0,
            "when scopes outnumber the global cap, global retention must be allowed to expire some scopes"
        );
        assert_eq!(
            watch_events_min_scope_rows_for_scope_count(1, 3),
            1,
            "small scope counts should still preserve one rare-scope row even with an aggressive cap"
        );
    }
}
