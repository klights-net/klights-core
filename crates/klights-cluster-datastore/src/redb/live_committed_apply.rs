//! Phase 10C.2 Redb status, ledger, watermark, and committed-apply ownership.
//!
//! Redb currently fails closed for the complete committed-apply transaction.
//! Its supported status and durable-ledger primitives remain grouped here so a
//! future move cannot accidentally activate only part of that transaction.

use std::num::NonZeroUsize;
use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::{Result, anyhow};
use serde_json::Value;

use super::RedbAccessor;
use super::key_codec::resource_key;
use super::mutation_helpers as helpers;
use super::read_core::RedbReadCore;
use super::tables;
use klights_cluster_core::{
    LogApplyAppliedOutboxRow, OutboxApplyError, OutboxStreamWatermark, Resource,
};
use klights_cluster_store::SnapshotOutboxWatermarkCursor;
use klights_cluster_store::{
    CommittedApplyError, CommittedApplyFuture, CommittedRaftApplyReceipt,
    CommittedRaftApplyRequest, PrivilegedCommittedRaftApply,
};

#[derive(Clone)]
pub struct RedbLiveCommittedApplyStore {
    accessor: Arc<RedbAccessor>,
}

impl RedbLiveCommittedApplyStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    pub async fn update_status(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        expected_rv: Option<i64>,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let name = name.to_string();
        let namespace = namespace.map(str::to_string);
        self.accessor
            .call("update_status_only", move |db| {
                let key = resource_key(&api_version, &kind, namespace.as_deref(), &name);
                let resource_table = if namespace.is_some() {
                    tables::RES_NS
                } else {
                    tables::RES_CLUSTER
                };
                let write = db.begin_write()?;
                let (old_body, current_rv) = {
                    let table = write.open_table(resource_table)?;
                    let row = table
                        .get(key.as_slice())?
                        .ok_or_else(|| anyhow!("not found"))?;
                    let current_rv = row.value().0 as i64;
                    if let Some(expected_rv) = expected_rv
                        && expected_rv > 0
                        && current_rv != expected_rv
                    {
                        return Err(crate::errors::DatastoreError::conflict(format!(
                            "rv conflict: expected {expected_rv} got {current_rv}"
                        ))
                        .into());
                    }
                    (row.value().1.to_vec(), current_rv)
                };
                let mut current: Value = serde_json::from_slice(&old_body).unwrap_or(Value::Null);
                if current.get("status") == Some(&status) {
                    let uid = Resource::uid_from_data(&current);
                    helpers::log_noop_resource_write(
                        "redb_update_status_only",
                        &api_version,
                        &kind,
                        namespace.as_deref(),
                        &name,
                        &uid,
                        current_rv,
                        "status unchanged",
                    );
                    write.commit()?;
                    return Ok((
                        Resource {
                            id: 0,
                            api_version,
                            kind,
                            namespace,
                            name,
                            uid,
                            resource_version: current_rv,
                            data: Arc::new(current),
                        },
                        None,
                    ));
                }
                if let Some(object) = current.as_object_mut() {
                    object.insert("status".to_string(), status);
                }
                let new_body = serde_json::to_vec(&current)?;
                let resource_version = helpers::incr_rv(&write)?;
                {
                    let mut table = write.open_table(resource_table)?;
                    table.insert(
                        key.as_slice(),
                        (resource_version as u64, new_body.as_slice()),
                    )?;
                }
                {
                    let mut rv_to_key = write.open_table(tables::RV_TO_KEY)?;
                    rv_to_key.insert(resource_version as u64, key.as_slice())?;
                }
                let event = serde_json::json!({
                    "apiVersion": api_version,
                    "kind": kind,
                    "namespace": namespace,
                    "name": name,
                    "eventType": "MODIFIED",
                    "data": current,
                });
                helpers::watch_insert(&write, resource_version, &event)?;
                write.commit()?;
                let pending = helpers::stage_resource_post_commit(
                    &api_version,
                    &kind,
                    namespace.as_deref(),
                    &name,
                    resource_version,
                    "MODIFIED",
                    current.clone(),
                );
                Ok((
                    Resource {
                        id: 0,
                        api_version,
                        kind,
                        namespace,
                        name,
                        uid: Resource::uid_from_data(&current),
                        resource_version,
                        data: Arc::new(current),
                    },
                    Some(pending),
                ))
            })
            .await
    }

    pub async fn update_status_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        status: Value,
        preconditions: klights_cluster_core::ResourcePreconditions,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        if let Some(expected_uid) = preconditions.uid.as_deref() {
            let Some(resource) = RedbReadCore::new(self.accessor.clone())
                .get_resource(api_version, kind, namespace, name)
                .await?
            else {
                return Err(anyhow!("not found"));
            };
            let actual_uid = resource
                .data
                .pointer("/metadata/uid")
                .and_then(Value::as_str);
            if actual_uid != Some(expected_uid) {
                return Err(
                    crate::errors::DatastoreError::conflict("UID precondition failed").into(),
                );
            }
        }
        self.update_status(
            api_version,
            kind,
            namespace,
            name,
            status,
            preconditions.resource_version,
        )
        .await
    }

    pub async fn applied_outbox_prunable_count(&self, cutoff_ms: i64) -> Result<usize> {
        self.accessor
            .call("redb_applied_outbox_prunable_count", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::APPLIED_OUTBOX)?;
                let mut count = 0usize;
                for row in table.iter()? {
                    let (_, value) = row?;
                    let record: LogApplyAppliedOutboxRow = serde_json::from_slice(value.value())?;
                    if record.first_seen_ms < cutoff_ms {
                        count += 1;
                    }
                }
                Ok(count)
            })
            .await
    }

    pub async fn list_outbox_watermarks(&self) -> Result<Vec<OutboxStreamWatermark>> {
        self.accessor
            .call("redb_outbox_stream_watermarks_list_all", |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::OUTBOX_STREAM_WATERMARKS)?;
                let mut rows = Vec::new();
                for entry in table.iter()? {
                    let (key, value) = entry?;
                    rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                }
                Ok(rows)
            })
            .await
    }

    pub async fn list_outbox_watermarks_paged(
        &self,
        after: Option<&SnapshotOutboxWatermarkCursor>,
        limit: NonZeroUsize,
    ) -> Result<Vec<OutboxStreamWatermark>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow!(
                "outbox-watermark page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after = after
            .map(|cursor| outbox_watermark_key(cursor.client_id(), cursor.stream_id()))
            .transpose()?;
        let limit = limit.get();
        self.accessor
            .call("redb_outbox_stream_watermarks_list_paged", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::OUTBOX_STREAM_WATERMARKS)?;
                let mut rows = Vec::with_capacity(limit);
                if let Some(after) = after.as_ref() {
                    for entry in table.range(after.as_slice()..)? {
                        let (key, value) = entry?;
                        if key.value() <= after.as_slice() {
                            continue;
                        }
                        rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                } else {
                    for entry in table.iter()? {
                        let (key, value) = entry?;
                        rows.push(decode_outbox_watermark_key(key.value(), value.value())?);
                        if rows.len() == limit {
                            break;
                        }
                    }
                }
                Ok(rows)
            })
            .await
    }

    pub async fn get_applied_outbox_bytes(&self, idempotency_key: &str) -> Result<Option<Vec<u8>>> {
        let key = idempotency_key.to_string();
        self.accessor
            .call("redb_get_applied_outbox", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::APPLIED_OUTBOX)?;
                Ok(table.get(key.as_str())?.map(|value| value.value().to_vec()))
            })
            .await
    }

    pub async fn insert_applied_outbox_bytes(
        &self,
        idempotency_key: String,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        self.accessor
            .call("redb_insert_applied_outbox", move |db| {
                let write = db.begin_write()?;
                let inserted = {
                    let mut table = write.open_table(tables::APPLIED_OUTBOX)?;
                    if table.get(idempotency_key.as_str())?.is_some() {
                        false
                    } else {
                        table.insert(idempotency_key.as_str(), bytes.as_slice())?;
                        true
                    }
                };
                write.commit()?;
                Ok(inserted)
            })
            .await
    }

    pub async fn list_applied_outbox_bytes(&self) -> Result<Vec<(String, Vec<u8>)>> {
        self.accessor
            .call("redb_list_applied_outbox", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::APPLIED_OUTBOX)?;
                let mut rows = Vec::new();
                for row in table.iter()? {
                    let (key, value) = row?;
                    rows.push((key.value().to_string(), value.value().to_vec()));
                }
                Ok(rows)
            })
            .await
    }

    pub async fn list_applied_outbox_bytes_paged(
        &self,
        after_key: Option<&str>,
        limit: NonZeroUsize,
    ) -> Result<Vec<Vec<u8>>> {
        if limit.get() > klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE {
            return Err(anyhow!(
                "applied-outbox page limit {} exceeds {}",
                limit,
                klights_cluster_store::MAX_SNAPSHOT_CAPTURE_PAGE
            ));
        }
        let after_key = after_key.map(str::to_owned);
        let limit = limit.get();
        self.accessor
            .call("redb_list_applied_outbox_paged", move |db| {
                let read = db.begin_read()?;
                let table = read.open_table(tables::APPLIED_OUTBOX)?;
                let mut rows = Vec::with_capacity(limit);
                if let Some(after_key) = after_key.as_deref() {
                    for row in table.range(after_key..)? {
                        let (key, value) = row?;
                        if key.value() <= after_key {
                            continue;
                        }
                        rows.push(value.value().to_vec());
                        if rows.len() == limit {
                            break;
                        }
                    }
                } else {
                    for row in table.iter()? {
                        let (_, value) = row?;
                        rows.push(value.value().to_vec());
                        if rows.len() == limit {
                            break;
                        }
                    }
                }
                Ok(rows)
            })
            .await
    }

    pub async fn gc_applied_outbox(&self, now_ms: i64, ttl_ms: i64) -> Result<usize> {
        let cutoff = now_ms.saturating_sub(ttl_ms);
        self.accessor
            .call("redb_applied_outbox_gc", move |db| {
                let write = db.begin_write()?;
                let keys_to_remove = {
                    let table = write.open_table(tables::APPLIED_OUTBOX)?;
                    let mut keys = Vec::new();
                    for row in table.iter()? {
                        let (key, value) = row?;
                        let record: LogApplyAppliedOutboxRow =
                            serde_json::from_slice(value.value())?;
                        if record.first_seen_ms < cutoff {
                            keys.push(key.value().to_string());
                        }
                    }
                    keys
                };
                let removed = {
                    let mut table = write.open_table(tables::APPLIED_OUTBOX)?;
                    let removed = keys_to_remove.len();
                    for key in keys_to_remove {
                        table.remove(key.as_str())?;
                    }
                    removed
                };
                write.commit()?;
                Ok(removed)
            })
            .await
    }

    pub async fn set_klights_meta(&self, key: &str, value: &str) -> Result<()> {
        let key = key.to_string();
        let value = value.to_string();
        self.accessor
            .call("redb_set_klights_meta", move |db| {
                let write = db.begin_write()?;
                {
                    let mut table = write.open_table(tables::KLIGHTS_META)?;
                    table.insert(key.as_str(), value.as_str())?;
                }
                write.commit()?;
                Ok(())
            })
            .await
    }

    pub fn apply_log_apply_commit<T>(&self) -> Result<T> {
        Err(anyhow!("backend does not support log-apply commit replay"))
    }

    pub fn apply_raft_log_apply_commit<T>(&self) -> Result<T> {
        Err(anyhow!(
            "redb backend does not support raft log-apply commit replay"
        ))
    }

    pub fn apply_raft_log_apply_commit_receipt<T>(&self) -> Result<T> {
        Err(anyhow!(
            "datastore backend does not support atomic committed-apply receipts"
        ))
    }

    pub fn apply_outbox_transactionally<T>(&self) -> std::result::Result<T, OutboxApplyError> {
        Err(OutboxApplyError::Retryable(
            "redb: apply_outbox_transactionally not implemented".to_string(),
        ))
    }

    pub fn apply_outbox_transactionally_with_watermark<T>(
        &self,
    ) -> std::result::Result<T, OutboxApplyError> {
        self.apply_outbox_transactionally()
    }

    pub fn apply_outbox_transactionally_with_watermark_effect<T>(
        &self,
    ) -> std::result::Result<T, OutboxApplyError> {
        self.apply_outbox_transactionally()
    }

    pub fn build_log_apply_commit_for_command<T>(&self) -> Result<T> {
        Err(anyhow!(
            "backend does not support generic raft commit materialization"
        ))
    }

    pub fn build_log_apply_commit_for_outbox<T>(&self) -> std::result::Result<T, OutboxApplyError> {
        Err(OutboxApplyError::Retryable(
            "redb: build_log_apply_commit_for_outbox not implemented".to_string(),
        ))
    }

    pub fn build_log_apply_commit_for_outbox_with_watermark<T>(
        &self,
    ) -> std::result::Result<T, OutboxApplyError> {
        self.build_log_apply_commit_for_outbox()
    }
}

impl PrivilegedCommittedRaftApply for RedbLiveCommittedApplyStore {
    fn apply_committed_raft(
        &self,
        _request: CommittedRaftApplyRequest,
    ) -> CommittedApplyFuture<'_, CommittedRaftApplyReceipt> {
        Box::pin(async move {
            self.apply_raft_log_apply_commit_receipt().map_err(|error| {
                CommittedApplyError::UnsupportedMode {
                    message: format!("{error:#}"),
                }
            })
        })
    }
}

pub fn outbox_watermark_key(client_id: &str, stream_id: i64) -> Result<Vec<u8>> {
    if client_id.is_empty() || client_id.contains('\0') || stream_id <= 0 {
        return Err(anyhow!(
            "outbox watermark requires a non-empty NUL-free client ID and positive stream ID"
        ));
    }
    let mut key = Vec::with_capacity(client_id.len() + 9);
    key.extend_from_slice(client_id.as_bytes());
    key.push(0);
    key.extend_from_slice(&(stream_id as u64).to_be_bytes());
    Ok(key)
}

pub fn decode_outbox_watermark_key(key: &[u8], stream_seq: i64) -> Result<OutboxStreamWatermark> {
    if key.len() < 10 || key[key.len() - 9] != 0 {
        return Err(anyhow!("corrupt redb outbox-watermark key"));
    }
    let client_id = std::str::from_utf8(&key[..key.len() - 9])
        .map_err(|error| anyhow!("corrupt redb outbox-watermark client ID: {error}"))?
        .to_string();
    let stream_id = u64::from_be_bytes(
        key[key.len() - 8..]
            .try_into()
            .expect("watermark key suffix is eight bytes"),
    );
    let stream_id =
        i64::try_from(stream_id).map_err(|_| anyhow!("redb outbox stream ID exceeds i64"))?;
    if stream_seq <= 0 {
        return Err(anyhow!("corrupt redb outbox stream sequence {stream_seq}"));
    }
    Ok(OutboxStreamWatermark {
        client_id,
        stream_id,
        stream_seq,
    })
}
