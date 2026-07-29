//! Ordinary K8s Namespace create/update/delete mutations for Redb.

use std::sync::Arc;

use ::redb::ReadableTable;
use anyhow::{Result, anyhow};
use serde_json::Value;

use super::super::helpers;
use klights_cluster_core::Resource;
use klights_cluster_datastore::redb::RedbAccessor;
use klights_cluster_datastore::redb::tables;

#[derive(Clone)]
pub struct RedbOrdinaryNamespaceStore {
    pub accessor: Arc<RedbAccessor>,
}

impl RedbOrdinaryNamespaceStore {
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
    // Namespace CRUD
    // -----------------------------------------------------------------------

    pub async fn create_namespace(
        &self,
        name: &str,
        data: Value,
    ) -> Result<(Resource, Option<klights_cluster_store::StagedPostCommit>)> {
        let name_owned = name.to_string();
        self.db_call_with_post_commit("create_ns", move |db| {
            let name: &str = &name_owned;
            let body = serde_json::to_vec(&data)?;
            let w = db.begin_write()?;
            let rv = helpers::incr_rv(&w)?;
            {
                let mut t = w.open_table(tables::NAMESPACES)?;
                if t.get(name)?.is_some() {
                    return Err(anyhow!("exists"));
                }
                t.insert(name, body.as_slice())?;
            }
            let ev = serde_json::json!({"apiVersion":"v1","kind":"Namespace","namespace":null,"name":name,"eventType":"ADDED","data":data});
            helpers::watch_insert(&w, rv, &ev)?;
            w.commit()?;
            let pending = helpers::stage_resource_post_commit(
                    "v1",
                    "Namespace",
                    None,
                    name,
                    rv,
                    "ADDED",
                    data.clone(),
                );
            Ok((Resource {
                id: 0,
                api_version: "v1".into(),
                kind: "Namespace".into(),
                namespace: None,
                name: name.into(),
                uid: Resource::uid_from_data(&data),
                resource_version: rv,
                data: Arc::new(data),
            }, Some(pending)))
        })
        .await
    }

    pub async fn update_namespace(
        &self,
        name: &str,
        data: Value,
        _expected_rv: i64,
    ) -> Result<Resource> {
        let name_owned = name.to_string();
        self.db_call("update_ns_impl", move |db| {
            let name: &str = &name_owned;
            let b = serde_json::to_vec(&data)?;
            let w = db.begin_write()?;
            let rv = helpers::incr_rv(&w)?;
            {
                let mut t = w.open_table(tables::NAMESPACES)?;
                if t.get(name)?.is_none() {
                    return Err(anyhow!("not found"));
                }
                t.insert(name, b.as_slice())?;
            }
            let ev = serde_json::json!({"apiVersion":"v1","kind":"Namespace","namespace":null,"name":name,"eventType":"MODIFIED","data":data});
            helpers::watch_insert(&w, rv, &ev)?;
            w.commit()?;
            Ok(Resource {
                id: 0,
                api_version: "v1".into(),
                kind: "Namespace".into(),
                namespace: None,
                name: name.into(),
                uid: Resource::uid_from_data(&data),
                resource_version: rv,
                data: Arc::new(data),
            })
        })
        .await
    }

    pub async fn delete_namespace(&self, name: &str) -> Result<()> {
        let name_owned = name.to_string();
        self.db_call("delete_ns_impl", move |db| {
            let name: &str = &name_owned;
            let w = db.begin_write()?;
            {
                let res = w.open_table(tables::RES_NS)?;
                for entry in res.iter()? {
                    let (_, val) = entry?;
                    let (rv, body) = val.value();
                    let body_owned = body.to_vec();
                    if let Some(resource) = helpers::resource_in_ns(&[], rv, &body_owned)
                        && resource.namespace.as_deref() == Some(name)
                    {
                        anyhow::bail!("Namespace has remaining content");
                    }
                }
            }
            {
                let mut t = w.open_table(tables::NAMESPACES)?;
                t.remove(name)?;
            }
            w.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn delete_namespace_contents(&self, namespace: &str) -> Result<()> {
        let namespace_owned = namespace.to_string();
        self.db_call("delete_namespace_contents_impl", move |db| {
            let namespace: &str = &namespace_owned;
            let w = db.begin_write()?;
            {
                let mut tbl = w.open_table(tables::RES_NS)?;
                let keys_to_delete: Vec<Vec<u8>> = tbl
                    .iter()?
                    .filter_map(|e| e.ok())
                    .filter(|(_, val)| {
                        let body = val.value().1;
                        let body_owned = body.to_vec();
                        helpers::resource_in_ns(&[], 0, &body_owned)
                            .map(|r| r.namespace.as_deref() == Some(namespace) && r.kind != "Pod")
                            .unwrap_or(false)
                    })
                    .map(|(k, _)| k.value().to_vec())
                    .collect();
                for key in &keys_to_delete {
                    tbl.remove(key.as_slice())?;
                }
            }
            w.commit()?;
            Ok(())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datastore::redb::crud::namespaces::RedbNamespaceStore;
    use crate::datastore::redb::crud::resources::RedbResourceStore;
    use klights_cluster_datastore::redb as open_boundary;
    use klights_cluster_datastore::redb::RedbAccessor;
    use klights_supervisor::TaskSupervisor;
    use serde_json::json;

    async fn store() -> RedbNamespaceStore {
        let supervisor = Arc::new(TaskSupervisor::new(Default::default()));
        let db = open_boundary::open_in_memory(supervisor.as_ref())
            .await
            .unwrap();
        let accessor = Arc::new(RedbAccessor::new(Arc::new(db), supervisor));
        RedbNamespaceStore::new(accessor)
    }

    #[tokio::test]
    async fn create_and_list_namespace() {
        let s = store().await;
        s.create_ns("testns", json!({"metadata":{"name":"testns"}}))
            .await
            .unwrap();
        let resources = s.list_namespace_resources_impl("testns").await.unwrap();
        assert!(resources.is_empty());
    }

    #[tokio::test]
    async fn create_duplicate_namespace_fails() {
        let s = store().await;
        s.create_ns("dupns", json!({"metadata":{"name":"dupns"}}))
            .await
            .unwrap();
        let err = s
            .create_ns("dupns", json!({"metadata":{"name":"dupns"}}))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("exists"));
    }

    #[tokio::test]
    async fn delete_namespace_fails_if_content_exists() {
        let s = store().await;
        s.create_ns("hascontent", json!({"metadata":{"name":"hascontent"}}))
            .await
            .unwrap();
        // Insert a resource into this namespace via the resource store.
        let resources = RedbResourceStore::new(
            s.accessor.clone(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        );
        resources.create_res("v1", "ConfigMap", Some("hascontent"), "cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"hascontent"}})).await.unwrap();
        let err = s.delete_ns_impl("hascontent").await.unwrap_err();
        assert!(err.to_string().contains("remaining content"));
    }

    #[tokio::test]
    async fn count_namespace_resources() {
        let s = store().await;
        s.create_ns("cnt", json!({"metadata":{"name":"cnt"}}))
            .await
            .unwrap();
        let resources = RedbResourceStore::new(
            s.accessor.clone(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        );
        resources
            .create_res(
                "v1",
                "Pod",
                Some("cnt"),
                "p1",
                json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p1","namespace":"cnt"}}),
            )
            .await
            .unwrap();
        resources
            .create_res(
                "v1",
                "Pod",
                Some("cnt"),
                "p2",
                json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p2","namespace":"cnt"}}),
            )
            .await
            .unwrap();
        assert_eq!(s.count_namespace_resources_impl("cnt").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn list_namespace_resources_excluding_kind() {
        let s = store().await;
        s.create_ns("excl", json!({"metadata":{"name":"excl"}}))
            .await
            .unwrap();
        let resources = RedbResourceStore::new(
            s.accessor.clone(),
            std::sync::Arc::new(klights_supervisor::SystemWallClock),
        );
        resources
            .create_res(
                "v1",
                "Pod",
                Some("excl"),
                "p",
                json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"p","namespace":"excl"}}),
            )
            .await
            .unwrap();
        resources.create_res("v1", "ConfigMap", Some("excl"), "cm",
            json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm","namespace":"excl"}})).await.unwrap();
        let excluding = s
            .list_namespace_resources_excluding_kind_impl("excl", "Pod")
            .await
            .unwrap();
        assert_eq!(excluding.len(), 1);
        assert_eq!(excluding[0].kind, "ConfigMap");
    }
}
