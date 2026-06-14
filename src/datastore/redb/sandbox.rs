//! `RedbSandboxStore` — pod sandbox lifecycle tracking.

use std::sync::Arc;

use ::redb::{ReadableDatabase, ReadableTable};
use anyhow::Result;
use serde_json::Value;

use crate::datastore::redb::accessor::RedbAccessor;
use crate::datastore::redb::key_codec::lex_next;
use crate::datastore::redb::tables;
use crate::datastore::types::*;

fn sandbox_key(ns: &str, pod: &str, uid: &str) -> String {
    format!("{}/{}/{}", ns, pod, uid)
}

fn sandbox_prefix(ns: &str, pod: &str) -> String {
    format!("{}/{}/", ns, pod)
}

pub struct RedbSandboxStore {
    pub accessor: Arc<RedbAccessor>,
}

impl RedbSandboxStore {
    pub fn new(accessor: Arc<RedbAccessor>) -> Self {
        Self { accessor }
    }

    async fn db_call<T, F>(&self, label: &str, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&::redb::Database) -> Result<T> + Send + 'static,
    {
        self.accessor.call(label, f).await
    }

    pub async fn record(&self, ns: &str, pod: &str, uid: &str, sid: &str) -> Result<()> {
        let ns_owned = ns.to_string();
        let pod_owned = pod.to_string();
        let sid_owned = sid.to_string();
        let uid_owned = uid.to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.db_call("record_sandbox_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod: &str = &pod_owned;
            let sid: &str = &sid_owned;
            let uid: &str = &uid_owned;
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::POD_SANDBOXES)?;
                let v = serde_json::json!({"sid": sid, "created_at": now});
                t.insert(
                    sandbox_key(ns, pod, uid).as_str(),
                    serde_json::to_vec(&v)?.as_slice(),
                )?;
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn get_for_pod(&self, ns: &str, pod: &str) -> Result<Option<String>> {
        let ns_owned = ns.to_string();
        let pod_owned = pod.to_string();
        self.db_call("get_sandbox_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod: &str = &pod_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_SANDBOXES)?;
            let prefix = sandbox_prefix(ns, pod);
            let end = String::from_utf8(
                lex_next(prefix.as_bytes()).expect("sandbox prefix must have lex successor"),
            )
            .expect("sandbox prefix successor must stay utf-8");
            let mut newest: Option<(i64, String)> = None;
            for entry in t.range(prefix.as_str()..end.as_str())? {
                let (_, value) = entry?;
                let parsed: Value = serde_json::from_slice(value.value()).unwrap_or_default();
                let created_at = parsed
                    .get("created_at")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let sandbox_id = parsed
                    .get("sid")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if newest
                    .as_ref()
                    .is_none_or(|(current_created_at, _)| created_at >= *current_created_at)
                {
                    newest = Some((created_at, sandbox_id));
                }
            }
            Ok(newest.map(|(_, sid)| sid))
        })
        .await
    }

    pub async fn get_for_uid(&self, ns: &str, pod: &str, uid: &str) -> Result<Option<String>> {
        let ns_owned = ns.to_string();
        let pod_owned = pod.to_string();
        let uid_owned = uid.to_string();
        self.db_call("get_sandbox_for_uid_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod: &str = &pod_owned;
            let uid: &str = &uid_owned;
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_SANDBOXES)?;
            Ok(t.get(sandbox_key(ns, pod, uid).as_str())?.and_then(|g| {
                let v: Value = serde_json::from_slice(g.value()).unwrap_or_default();
                v.get("sid").and_then(|s| s.as_str()).map(|s| s.to_string())
            }))
        })
        .await
    }

    pub async fn delete_for_pod(&self, ns: &str, pod: &str) -> Result<()> {
        let ns_owned = ns.to_string();
        let pod_owned = pod.to_string();
        self.db_call("del_sandbox_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod: &str = &pod_owned;
            let w = db.begin_write()?;
            {
                let mut table = w.open_table(tables::POD_SANDBOXES)?;
                let prefix = sandbox_prefix(ns, pod);
                let end = String::from_utf8(
                    lex_next(prefix.as_bytes()).expect("sandbox prefix must have lex successor"),
                )
                .expect("sandbox prefix successor must stay utf-8");
                let keys: Vec<String> = table
                    .range(prefix.as_str()..end.as_str())?
                    .filter_map(|entry| entry.ok().map(|(key, _)| key.value().to_string()))
                    .collect();
                for key in keys {
                    table.remove(key.as_str())?;
                }
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn delete_for_uid(&self, ns: &str, pod: &str, uid: &str, sid: &str) -> Result<()> {
        let ns_owned = ns.to_string();
        let pod_owned = pod.to_string();
        let uid_owned = uid.to_string();
        let sid_owned = sid.to_string();
        self.db_call("del_sandbox_for_uid_impl", move |db| {
            let ns: &str = &ns_owned;
            let pod: &str = &pod_owned;
            let uid: &str = &uid_owned;
            let sid: &str = &sid_owned;
            let w = db.begin_write()?;
            {
                let mut t = w.open_table(tables::POD_SANDBOXES)?;
                let key = sandbox_key(ns, pod, uid);
                let should_remove = t.get(key.as_str())?.is_some_and(|g| {
                    let v: Value = serde_json::from_slice(g.value()).unwrap_or_default();
                    v.get("sid").and_then(|s| s.as_str()) == Some(sid)
                });
                if should_remove {
                    t.remove(key.as_str())?;
                }
            }
            Ok(w.commit()?)
        })
        .await
    }

    pub async fn list_all(&self) -> Result<Vec<SandboxRef>> {
        self.db_call("list_sandboxes_impl", move |db| {
            let r = db.begin_read()?;
            let t = r.open_table(tables::POD_SANDBOXES)?;
            let mut items = Vec::new();
            for e in t.iter()? {
                let (k, v) = e?;
                let key = k.value();
                let parts: Vec<&str> = key.splitn(3, '/').collect();
                let val: Value = serde_json::from_slice(v.value()).unwrap_or_default();
                items.push(SandboxRef {
                    namespace: parts.first().copied().unwrap_or("").to_string(),
                    pod_name: parts.get(1).copied().unwrap_or("").to_string(),
                    pod_uid: parts.get(2).copied().unwrap_or("").to_string(),
                    sandbox_id: val
                        .get("sid")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                });
            }
            Ok(items)
        })
        .await
    }
}
