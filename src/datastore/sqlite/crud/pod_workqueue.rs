use super::super::queries;
use super::*;
use crate::pod_identity::PodIdentity;
use rusqlite::OptionalExtension;

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

impl Datastore {
    pub async fn pod_workqueue_enqueue(
        &self,
        kind: PodWorkqueueKind,
        pod: &PodIdentity,
        payload: Value,
        attempt_count: i64,
        min_delay_ms: i64,
        last_error: Option<&str>,
    ) -> Result<()> {
        let kind = kind.as_str().to_string();
        let namespace = pod.namespace.clone();
        let name = pod.name.clone();
        let uid = pod.uid.clone();
        let payload = serde_json::to_vec(&payload)?;
        let last_error = last_error.map(|s| s.to_string());
        let now = now_ms();
        let floor = now.saturating_add(min_delay_ms.max(0));

        self.node_db_call("db_query", move |conn| {
            let tail_other: i64 = conn.query_row(
                queries::POD_WORKQUEUE_TAIL_OTHER,
                rusqlite::params![kind, namespace, name, uid],
                |row| row.get(0),
            )?;
            let next_attempt_at_ms = floor.max(tail_other.saturating_add(1));
            conn.execute(
                queries::POD_WORKQUEUE_UPSERT,
                rusqlite::params![
                    kind,
                    namespace,
                    name,
                    uid,
                    payload,
                    attempt_count,
                    next_attempt_at_ms,
                    last_error,
                    now
                ],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("pod_workqueue enqueue failed: {}", e))
    }

    pub async fn pod_workqueue_peek_next_due(&self) -> Result<Option<i64>> {
        self.node_db_call("db_query", move |conn| {
            conn.query_row(queries::POD_WORKQUEUE_PEEK_NEXT_DUE, [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .optional()
            .map(|v| v.flatten())
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .map_err(|e| anyhow::anyhow!("pod_workqueue peek failed: {}", e))
    }

    pub async fn pod_workqueue_claim_due(&self, now_ms: i64) -> Result<Option<PodWorkqueueEntry>> {
        self.node_db_call("db_query", move |conn| {
            let tx = conn.transaction()?;
            let row = tx
                .query_row(
                    queries::POD_WORKQUEUE_CLAIM_DUE,
                    rusqlite::params![now_ms],
                    |row| {
                        let kind_raw: String = row.get(1)?;
                        let kind = PodWorkqueueKind::parse(&kind_raw).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                1,
                                rusqlite::types::Type::Text,
                                Box::new(std::io::Error::other(e.to_string())),
                            )
                        })?;
                        let payload: Vec<u8> = row.get(5)?;
                        let payload = serde_json::from_slice::<Value>(&payload).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Blob,
                                Box::new(e),
                            )
                        })?;
                        Ok(PodWorkqueueEntry {
                            id: row.get(0)?,
                            kind,
                            namespace: row.get(2)?,
                            name: row.get(3)?,
                            uid: row.get(4)?,
                            payload,
                            attempt_count: row.get(6)?,
                            next_attempt_at_ms: row.get(7)?,
                        })
                    },
                )
                .optional()?;

            if let Some(ref claimed) = row {
                tx.execute(
                    queries::POD_WORKQUEUE_DELETE_BY_ID,
                    rusqlite::params![claimed.id],
                )?;
            }
            tx.commit()?;
            Ok(row)
        })
        .await
        .map_err(|e| anyhow::anyhow!("pod_workqueue claim failed: {}", e))
    }

    pub async fn pod_workqueue_complete(&self, id: i64) -> Result<()> {
        self.node_db_call("db_query", move |conn| {
            conn.execute(queries::POD_WORKQUEUE_DELETE_BY_ID, rusqlite::params![id])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("pod_workqueue complete failed: {}", e))
    }

    pub async fn pod_workqueue_record_failure(
        &self,
        row: PodWorkqueueEntry,
        min_delay_ms: i64,
        error: &str,
    ) -> Result<()> {
        let pod = PodIdentity::new(&row.namespace, &row.name, &row.uid);
        self.pod_workqueue_enqueue(
            row.kind,
            &pod,
            row.payload,
            row.attempt_count.saturating_add(1),
            min_delay_ms,
            Some(error),
        )
        .await
    }

    pub async fn pod_workqueue_dead_letter(&self, id: i64, _error: &str) -> Result<()> {
        self.pod_workqueue_complete(id).await
    }
}
