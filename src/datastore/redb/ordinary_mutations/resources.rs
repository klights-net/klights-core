//! Ordinary K8s resource create/update/delete/patch mutations for Redb.
//!
//! All methods operate through `RedbAccessor` for supervised DB access.
//! Owner-reference and watch-event side effects are handled inline.

use std::sync::Arc;

use ::redb::ReadableTable;
use anyhow::{Result, anyhow};
use serde_json::Value;

use super::super::helpers;
#[cfg(test)]
use crate::datastore::types::ReplicatedCreateOptions;
use klights_cluster_core::{Resource, ResourcePatchRequest, ResourcePreconditions};
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_datastore::redb::key_codec::resource_key;
use klights_cluster_datastore::redb::read_core::RedbReadCore;
use klights_cluster_datastore::redb::tables;

#[derive(Clone)]
pub struct RedbOrdinaryResourceStore {
    accessor: Arc<RedbAccessor>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl RedbOrdinaryResourceStore {
    pub fn new(
        accessor: Arc<RedbAccessor>,
        wall_clock: Arc<dyn klights_supervisor::WallClock>,
    ) -> Self {
        Self {
            accessor,
            wall_clock,
        }
    }

    /// Run a synchronous redb closure on the DB-category blocking pool.
    #[cfg(test)]
    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    async fn db_call_with_post_commit<T, F>(
        &self,
        label: &str,
        f: F,
    ) -> Result<(T, Option<klights_cluster_store::StagedPostCommit>)>
    where
        T: Send + 'static,
        F: FnOnce(
                &::redb::Database,
            ) -> Result<(T, Option<klights_cluster_store::StagedPostCommit>)>
            + Send
            + 'static,
    {
        self.accessor.call(label, f).await
    }

    // -----------------------------------------------------------------------
    // Resource CRUD
    // -----------------------------------------------------------------------

    pub async fn create_resource(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        mut data: Value,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
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
        let (rv, pending) = self
            .db_call_with_post_commit("create_res", move |db| {
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
                let pending = helpers::stage_resource_post_commit(
                        &av_owned,
                        &kind_owned,
                        ns_owned.as_deref(),
                        &name_owned,
                        rv,
                        "ADDED",
                        data,
                    );
                Ok((rv, Some(pending)))
            })
            .await?;
        Ok((
            Resource {
                id: 0,
                api_version: av_res,
                kind: kind_res,
                namespace: ns_res,
                name: name_res,
                uid: Resource::uid_from_data(&data_clone),
                resource_version: rv,
                data: data_clone,
            },
            pending,
        ))
    }

    #[cfg(test)]
    pub async fn apply_replicated_create_resource(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        mut data: Value,
        options: ReplicatedCreateOptions,
    ) -> Result<(Resource, Vec<klights_cluster_store::StagedPostCommit>)> {
        let ReplicatedCreateOptions {
            resource_version,
            meta_uid,
        } = options;
        if resource_version <= 0 {
            return Err(anyhow!(
                "replicated create resourceVersion must be positive"
            ));
        }
        helpers::ensure_uid(&mut data);
        let incoming_uid = helpers::resource_uid(&data)
            .map(|value| value.to_string())
            .unwrap_or_default();
        if let Some(expected_uid) = meta_uid.as_deref()
            && expected_uid != incoming_uid
        {
            return Err(klights_cluster_datastore::errors::DatastoreError::conflict(format!(
                "replicated create UID precondition failed: expected {expected_uid} got {incoming_uid}"
            ))
            .into());
        }

        let data_for_return = Arc::new(data.clone());
        let data_for_publish = data.clone();
        let data_bytes = serde_json::to_vec(&data_for_publish)?;
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|value| value.to_string());
        let name_owned = name.to_string();
        let incoming_uid_for_db = incoming_uid.clone();
        let av_for_db = av_owned.clone();
        let kind_for_db = kind_owned.clone();
        let ns_for_db = ns_owned.clone();
        let name_for_db = name_owned.clone();
        let event_payload_for_db = data_for_publish;

        enum ApplyCreateResult {
            AlreadyReflected {
                current_rv: i64,
                current_data: Vec<u8>,
            },
            Inserted,
            UpdatedSameUid,
            ReplacedDifferentUid {
                old_data: Vec<u8>,
                deleted_rv: i64,
            },
        }

        let outcome = self
            .db_call("apply_replicated_create_resource", move |db| {
                let w = db.begin_write()?;
                let av_o = av_for_db;
                let kind_o = kind_for_db;
                let ns_o = ns_for_db;
                let name_o = name_for_db;
                let event_payload = event_payload_for_db;
                let data_bytes = data_bytes;
                let resource_version = resource_version;
                let incoming_uid_for_db = incoming_uid_for_db;
                let key = key;
                let res_tbl = if ns_o.is_some() {
                    tables::RES_NS
                } else {
                    tables::RES_CLUSTER
                };
                let existing = {
                    let table = w.open_table(res_tbl)?;
                    table.get(key.as_slice())?.map(|entry| {
                        let (current_rv, old_body) = entry.value();
                        (current_rv, old_body.to_vec())
                    })
                };
                let outcome = match existing {
                    None => {
                        {
                            let mut table = w.open_table(res_tbl)?;
                            table.insert(
                                key.as_slice(),
                                (resource_version as u64, data_bytes.as_slice()),
                            )?;
                        }
                        {
                            let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                            rvk.insert(resource_version as u64, key.as_slice())?;
                        }
                        helpers::watch_insert(
                            &w,
                            resource_version,
                            &serde_json::json!({
                                "apiVersion":av_o,
                                "kind":kind_o,
                                "namespace":ns_o,
                                "name":name_o,
                                "eventType":"ADDED",
                                "data":event_payload.clone(),
                            }),
                        )?;
                        helpers::update_owner_table(
                            &w,
                            &av_o,
                            &kind_o,
                            ns_o.as_deref(),
                            &name_o,
                            None,
                            Some(&data_bytes),
                        )?;
                        ApplyCreateResult::Inserted
                    }
                    Some(existing) => {
                        let (current_rv, old_body) = existing;
                        let current_data: Value =
                            serde_json::from_slice(&old_body).unwrap_or(Value::Null);
                        let current_uid = Resource::uid_from_data(&current_data);
                        if current_uid == incoming_uid_for_db {
                            if current_rv >= resource_version as u64 {
                                ApplyCreateResult::AlreadyReflected {
                                    current_rv: current_rv as i64,
                                    current_data: old_body,
                                }
                            } else {
                                {
                                    let mut table = w.open_table(res_tbl)?;
                                    table.insert(
                                        key.as_slice(),
                                        (resource_version as u64, data_bytes.as_slice()),
                                    )?;
                                }
                                {
                                    let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                                    rvk.insert(resource_version as u64, key.as_slice())?;
                                }
                                helpers::watch_insert(
                                    &w,
                                    resource_version,
                                    &serde_json::json!({
                                        "apiVersion":av_o,
                                        "kind":kind_o,
                                        "namespace":ns_o,
                                        "name":name_o,
                                        "eventType":"MODIFIED",
                                        "data":event_payload.clone(),
                                    }),
                                )?;
                                helpers::update_owner_table(
                                    &w,
                                    &av_o,
                                    &kind_o,
                                    ns_o.as_deref(),
                                    &name_o,
                                    Some(&old_body),
                                    Some(&data_bytes),
                                )?;
                                ApplyCreateResult::UpdatedSameUid
                            }
                        } else {
                            let deleted_rv = resource_version.saturating_sub(1);
                            {
                                let mut table = w.open_table(res_tbl)?;
                                table.remove(key.as_slice())?;
                                table.insert(
                                    key.as_slice(),
                                    (resource_version as u64, data_bytes.as_slice()),
                                )?;
                            }
                            {
                                let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                                rvk.insert(resource_version as u64, key.as_slice())?;
                            }
                            helpers::watch_insert(
                                &w,
                                deleted_rv,
                                &serde_json::json!({
                                    "apiVersion":av_o,
                                    "kind":kind_o,
                                    "namespace":ns_o,
                                    "name":name_o,
                                    "eventType":"DELETED",
                                    "data":current_data,
                                }),
                            )?;
                            helpers::watch_insert(
                                &w,
                                resource_version,
                                &serde_json::json!({
                                    "apiVersion":av_o,
                                    "kind":kind_o,
                                    "namespace":ns_o,
                                    "name":name_o,
                                    "eventType":"ADDED",
                                    "data":event_payload.clone(),
                                }),
                            )?;
                            helpers::update_owner_table(
                                &w,
                                &av_o,
                                &kind_o,
                                ns_o.as_deref(),
                                &name_o,
                                Some(&old_body),
                                Some(&data_bytes),
                            )?;
                            ApplyCreateResult::ReplacedDifferentUid {
                                old_data: old_body,
                                deleted_rv,
                            }
                        }
                    }
                };
                w.commit()?;
                Ok(outcome)
            })
            .await?;

        match outcome {
            ApplyCreateResult::AlreadyReflected {
                current_rv,
                current_data,
            } => {
                let current_data: Value =
                    serde_json::from_slice(&current_data).unwrap_or(Value::Null);
                Ok((
                    Resource {
                        id: 0,
                        api_version: av_owned,
                        kind: kind_owned,
                        namespace: ns_owned,
                        name: name_owned,
                        uid: Resource::uid_from_data(&current_data),
                        resource_version: current_rv,
                        data: Arc::new(current_data),
                    },
                    Vec::new(),
                ))
            }
            ApplyCreateResult::Inserted => {
                let pending = helpers::stage_resource_post_commit(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    resource_version,
                    "ADDED",
                    (*data_for_return).clone(),
                );
                Ok((
                    Resource {
                        id: 0,
                        api_version: av_owned,
                        kind: kind_owned,
                        namespace: ns_owned,
                        name: name_owned,
                        uid: incoming_uid,
                        resource_version,
                        data: data_for_return,
                    },
                    vec![pending],
                ))
            }
            ApplyCreateResult::UpdatedSameUid => {
                let pending = helpers::stage_resource_post_commit(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    resource_version,
                    "MODIFIED",
                    (*data_for_return).clone(),
                );
                Ok((
                    Resource {
                        id: 0,
                        api_version: av_owned,
                        kind: kind_owned,
                        namespace: ns_owned,
                        name: name_owned,
                        uid: incoming_uid,
                        resource_version,
                        data: data_for_return,
                    },
                    vec![pending],
                ))
            }
            ApplyCreateResult::ReplacedDifferentUid {
                old_data,
                deleted_rv,
            } => {
                let old_object: Value = serde_json::from_slice(&old_data).unwrap_or(Value::Null);
                let deleted = helpers::stage_resource_post_commit(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    deleted_rv,
                    "DELETED",
                    old_object,
                );
                let added = helpers::stage_resource_post_commit(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    resource_version,
                    "ADDED",
                    (*data_for_return).clone(),
                );
                Ok((
                    Resource {
                        id: 0,
                        api_version: av_owned,
                        kind: kind_owned,
                        namespace: ns_owned,
                        name: name_owned,
                        uid: incoming_uid,
                        resource_version,
                        data: data_for_return,
                    },
                    vec![deleted, added],
                ))
            }
        }
    }

    pub async fn update_resource(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        data: Value,
        expected_rv: i64,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        self.update_resource_with_preconditions(
            av,
            kind,
            ns,
            name,
            data,
            ResourcePreconditions::resource_version(expected_rv),
        )
        .await
    }

    pub async fn update_resource_with_preconditions(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        data: Value,
        preconditions: ResourcePreconditions,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        self.db_call_with_post_commit("update_res", move |db: &::redb::Database| {
            let mut data = data.clone();
            let key = key.clone();
            let av_o = av_owned.clone();
            let kind_o = kind_owned.clone();
            let ns_o = ns_owned.clone();
            let name_o = name_owned.clone();
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

            if klights_cluster_core::resource_bodies_equal_ignoring_metadata_field(
                &current,
                &data,
                "resourceVersion",
            ) {
                let uid = Resource::uid_from_data(&current);
                helpers::log_noop_resource_write(
                    "redb_update_resource",
                    &av_o,
                    &kind_o,
                    ns_o.as_deref(),
                    &name_o,
                    &uid,
                    cur_rv,
                    "object unchanged",
                );
                w.commit()?;
                return Ok((Resource {
                    id: 0,
                    api_version: av_o,
                    kind: kind_o,
                    namespace: ns_o,
                    name: name_o,
                    uid,
                    resource_version: cur_rv,
                    data: Arc::new(current),
                }, None));
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
            let pending = helpers::stage_resource_post_commit(
                    &av_o,
                    &kind_o,
                    ns_o.as_deref(),
                    &name_o,
                    rv,
                    "MODIFIED",
                    data.clone(),
                );
            Ok((Resource {
                id: 0,
                api_version: av_o,
                kind: kind_o,
                namespace: ns_o,
                name: name_o,
                uid: Resource::uid_from_data(&data),
                resource_version: rv,
                data: Arc::new(data),
            }, Some(pending)))
        })
        .await
    }

    pub async fn delete_resource(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
    ) -> Result<((), Option<klights_cluster_store::StagedPostCommit>)> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        self.db_call_with_post_commit("delete_res", move |db| {
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
                    None => return Ok(((), None)),
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
            let pending = helpers::stage_resource_post_commit(
                    &av_owned,
                    &kind_owned,
                    ns_owned.as_deref(),
                    &name_owned,
                    rv,
                    "DELETED",
                    data,
                );
            Ok(((), Some(pending)))
        })
        .await
    }

    pub async fn delete_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
    ) -> Result<((), Option<klights_cluster_store::StagedPostCommit>)> {
        if preconditions.uid.is_some() || preconditions.resource_version.is_some() {
            let Some(resource) = RedbReadCore::new(self.accessor.clone())
                .get_resource(api_version, kind, namespace, name)
                .await?
            else {
                return Err(anyhow!("not found"));
            };
            if let Some(expected_uid) = preconditions.uid.as_deref() {
                let actual_uid = resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str);
                if actual_uid != Some(expected_uid) {
                    return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = preconditions.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.delete_resource(api_version, kind, namespace, name)
            .await
    }

    pub async fn delete_resource_with_tombstone(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        preconditions: ResourcePreconditions,
        grace_seconds: i64,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        let key = resource_key(av, kind, ns, name);
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        let name_owned = name.to_string();
        let av_error = av_owned.clone();
        let kind_error = kind_owned.clone();
        let ns_error = ns_owned.clone();
        let name_error = name_owned.clone();
        let av_event = av_owned.clone();
        let kind_event = kind_owned.clone();
        let ns_event = ns_owned.clone();
        let name_event = name_owned.clone();
        let deletion_timestamp =
            klights_cluster_core::k8s_time::format_legacy_timestamp(self.wall_clock.now_utc());

        let ((resource_version, data, uid), pending) = self
            .db_call_with_post_commit("delete_res_with_tombstone", move |db| {
                let res_tbl = if ns_error.is_some() {
                    tables::RES_NS
                } else {
                    tables::RES_CLUSTER
                };
                let w = db.begin_write()?;

                let (old_body, data, _) = {
                    let table = w.open_table(res_tbl)?;
                    let Some(current_row) = table.get(key.as_slice())? else {
                        return Err(klights_cluster_datastore::errors::DatastoreError::not_found(format!(
                            "delete_resource_without_watch_with_tombstone: {av_error}/{kind_error}/{name_error} not found"
                        ))
                        .into());
                    };
                    let body = current_row.value().1.to_vec();
                    let current_rv = current_row.value().0 as i64;
                    let mut current = serde_json::from_slice::<Value>(&body)?;
                    helpers::validate_resource_preconditions(&preconditions, &current, current_rv)?;
                    let Some(metadata) = current.get_mut("metadata").and_then(Value::as_object_mut) else {
                        return Err(anyhow!(
                            "delete_resource_without_watch_with_tombstone: {av_error}/{kind_error}/{name_error} is missing metadata"
                        ));
                    };
                    if metadata
                        .get("deletionTimestamp")
                        .and_then(Value::as_str)
                        .is_none_or(str::is_empty)
                    {
                        metadata.insert(
                            "deletionTimestamp".to_string(),
                            serde_json::Value::String(deletion_timestamp),
                        );
                    }
                    metadata
                        .entry("deletionGracePeriodSeconds".to_string())
                        .or_insert_with(|| Value::from(grace_seconds));

                    (body, current, current_rv)
                };

                let watch_data = data.clone();
                let resource_uid = Resource::uid_from_data(&data);
                let rv = helpers::incr_rv(&w)?;
                {
                    let mut r = w.open_table(res_tbl)?;
                    r.remove(key.as_slice())?;
                }
                {
                    let mut rvk = w.open_table(tables::RV_TO_KEY)?;
                    rvk.remove(rv as u64)?;
                }
                let ev = serde_json::json!({
                    "apiVersion":av_event,
                    "kind":kind_event,
                    "namespace":ns_event,
                    "name":name_event,
                    "eventType":"DELETED",
                    "data":data,
                });
                helpers::watch_insert(&w, rv, &ev)?;
                helpers::update_owner_table(
                    &w,
                    &av_event,
                    &kind_event,
                    ns_event.as_deref(),
                    &name_event,
                    Some(&old_body),
                    None,
                )?;
                w.commit()?;
                let pending = helpers::stage_resource_post_commit(
                        &av_event,
                        &kind_event,
                        ns_event.as_deref(),
                        &name_event,
                        rv,
                        "DELETED",
                        watch_data,
                    );
                Ok::<_, anyhow::Error>(((rv, Arc::new(data), resource_uid), Some(pending)))
            })
            .await?;

        Ok((
            Resource {
                id: 0,
                api_version: av_owned,
                kind: kind_owned,
                namespace: ns_owned,
                name: name_owned,
                uid,
                resource_version,
                data,
            },
            pending,
        ))
    }

    // -----------------------------------------------------------------------
    // Patch
    // -----------------------------------------------------------------------

    pub async fn patch_resource(
        &self,
        av: &str,
        kind: &str,
        ns: Option<&str>,
        name: &str,
        patch: Value,
    ) -> Result<(
        Option<Resource>,
        Option<klights_cluster_store::StagedPostCommit>,
    )> {
        let av_owned = av.to_string();
        let kind_owned = kind.to_string();
        let name_owned = name.to_string();
        let ns_owned = ns.map(|s| s.to_string());
        self.db_call_with_post_commit("patch", move |db| {
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
                    None => return Ok((None, None)),
                    Some(g) => {
                        let (rv, body) = g.value();
                        (Some(body.to_vec()), (rv, body.to_vec()))
                    }
                }
            };
            let mut current_data: Value =
                serde_json::from_slice(&current.1).unwrap_or(Value::Null);
            let before_patch = current_data.clone();
            klights_types::apply_merge_patch(&mut current_data, &patch);
            helpers::validate_uid_immutable(&current_data, &before_patch)?;
            if klights_cluster_core::resource_bodies_equal_ignoring_metadata_field(
                &before_patch,
                &current_data,
                "resourceVersion",
            ) {
                let uid = Resource::uid_from_data(&before_patch);
                helpers::log_noop_resource_write(
                    "redb_patch_resource_latest",
                    av,
                    kind,
                    ns,
                    name,
                    &uid,
                    current.0 as i64,
                    "patch result unchanged",
                );
                w.commit()?;
                return Ok((Some(Resource {
                    id: 0,
                    api_version: av.into(),
                    kind: kind.into(),
                    namespace: ns.map(|s| s.into()),
                    name: name.into(),
                    uid,
                    resource_version: current.0 as i64,
                    data: Arc::new(before_patch),
                }), None));
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
            let pending = helpers::stage_resource_post_commit(
                    av,
                    kind,
                    ns,
                    name,
                    rv,
                    "MODIFIED",
                    current_data.clone(),
                );
            Ok((Some(Resource {
                id: 0,
                api_version: av.into(),
                kind: kind.into(),
                namespace: ns.map(|s| s.into()),
                name: name.into(),
                uid: Resource::uid_from_data(&current_data),
                resource_version: rv,
                data: Arc::new(current_data),
            }), Some(pending)))
        })
        .await
    }

    pub async fn patch_resource_with_preconditions(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        name: &str,
        request: ResourcePatchRequest,
    ) -> Result<(
        Option<Resource>,
        Option<klights_cluster_store::StagedPostCommit>,
    )> {
        let ResourcePatchRequest {
            patch_kind: _,
            patch,
            preconditions,
            strict_resource_version: _,
        } = request;
        if preconditions.uid.is_some() || preconditions.resource_version.is_some() {
            let Some(resource) = RedbReadCore::new(self.accessor.clone())
                .get_resource(api_version, kind, namespace, name)
                .await?
            else {
                return Ok((None, None));
            };
            if let Some(expected_uid) = preconditions.uid.as_deref() {
                let actual_uid = resource
                    .data
                    .pointer("/metadata/uid")
                    .and_then(Value::as_str);
                if actual_uid != Some(expected_uid) {
                    return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                        "UID precondition failed",
                    )
                    .into());
                }
            }
            if let Some(expected_rv) = preconditions.resource_version
                && resource.resource_version != expected_rv
            {
                return Err(klights_cluster_datastore::errors::DatastoreError::conflict(
                    "resourceVersion precondition failed",
                )
                .into());
            }
        }
        self.patch_resource(api_version, kind, namespace, name, patch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datastore::redb::crud::resources::RedbResourceStore;
    use serde_json::json;

    use klights_cluster_datastore::redb as open_boundary;
    use klights_cluster_datastore::redb::RedbAccessor;
    use klights_supervisor::TaskSupervisor;

    use super::*;

    async fn store() -> RedbResourceStore {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = open_boundary::open_in_memory(supervisor.as_ref())
            .await
            .unwrap();
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbResourceStore::new(accessor, Arc::new(klights_supervisor::SystemWallClock))
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
        let s = store().await;
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
        let s = store().await;
        s.delete_res("v1", "Pod", Some("default"), "ghost")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn get_res_returns_none_for_missing() {
        let s = store().await;
        let r = s
            .get_res("v1", "Pod", Some("default"), "nope")
            .await
            .unwrap();
        assert!(r.is_none());
    }

    #[tokio::test]
    async fn watch_events_emitted_on_create_update_delete() {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = open_boundary::open_in_memory(supervisor.as_ref())
            .await
            .unwrap();
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        let s = RedbResourceStore::new(accessor, Arc::new(klights_supervisor::SystemWallClock));

        let pod =
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"we","namespace":"default"}});
        let (created, pending) = s
            .create_res("v1", "Pod", Some("default"), "we", pod)
            .await
            .unwrap();
        let pending = pending.unwrap();
        assert_eq!(pending.test_event().unwrap().event_type(), "ADDED");

        let (_, pending) = s.update_res("v1", "Pod", Some("default"), "we",
            json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"we","namespace":"default","labels":{"x":"y"}}}),
            created.resource_version).await.unwrap();
        let pending = pending.unwrap();
        assert_eq!(pending.test_event().unwrap().event_type(), "MODIFIED");

        let (_, pending) = s
            .delete_res("v1", "Pod", Some("default"), "we")
            .await
            .unwrap();
        let pending = pending.unwrap();
        assert_eq!(pending.test_event().unwrap().event_type(), "DELETED");
    }

    #[tokio::test]
    async fn redb_selector_limit_does_not_decode_all_rows() {
        let s = store().await;
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
        let s = store().await;
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
        let s = store().await;
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
        let s = store().await;
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
        let s = store().await;
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
