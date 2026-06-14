use super::super::queries;
use super::*;
impl Datastore {
    /// Find resources owned by a given owner UID via ownerReferences
    pub async fn find_owned_resources(
        &self,
        owner_uid: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        tracing::debug!(
            "find_owned_resources: owner_uid={} namespace={:?}",
            owner_uid,
            namespace
        );
        let owner_uid = owner_uid.to_string();
        let namespace_owned = namespace.map(str::to_string);

        // Match owner UID across any ownerReferences[*].uid entry.
        // This is correctness-critical for GC cascade walks; relying on
        // ownerReferences[0] misses valid dependents when the target ownerRef
        // is not in position 0.
        let mut items = Vec::new();

        let namespaced = self
            .db_call("db_query", {
                let namespace = namespace_owned.clone();
                let uid = owner_uid.clone();
                move |conn| {
                    let mut query = queries::OWNERSHIP_INDEXED_NAMESPACED_BY_UID.to_string();
                    let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(uid)];

                    if let Some(ref ns) = namespace {
                        query.push_str(&format!(" AND r.namespace = ?{}", params.len() + 1));
                        params.push(Box::new(ns.clone()));
                    }

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
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                }
            })
            .await?;
        items.extend(namespaced);

        // cluster_resources walk only when namespace is None — namespaced parents
        // never own cluster-scoped children.
        if namespace_owned.is_none() {
            let uid = owner_uid.clone();
            let cluster = self
                .db_call("db_query", move |conn| {
                    let mut stmt = conn.prepare(queries::OWNERSHIP_INDEXED_CLUSTER_BY_UID)?;
                    let rows = stmt.query_map([&uid], |row| {
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
                    })?;
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                })
                .await?;
            items.extend(cluster);
        }

        // Keep a defensive filter in Rust to guard malformed rows.
        let filtered: Vec<Resource> = items
            .into_iter()
            .filter(|item| {
                item.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|r| r.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|r| r.get("uid").and_then(|u| u.as_str()) == Some(&owner_uid))
                    })
                    .unwrap_or(false)
            })
            .collect();

        tracing::debug!(
            "find_owned_resources: {} matches for {}",
            filtered.len(),
            owner_uid
        );
        Ok(filtered)
    }

    /// Return all resources of `kind` whose ownerReferences contain
    /// `owner_uid` at any array position.
    pub async fn list_resources_by_owner_uid(
        &self,
        api_version: &str,
        kind: &str,
        namespace: Option<&str>,
        owner_uid: &str,
    ) -> Result<Vec<Resource>> {
        let api_version = api_version.to_string();
        let kind = kind.to_string();
        let namespace_owned = namespace.map(str::to_string);
        let owner_uid = owner_uid.to_string();
        let owner_uid_for_filter = owner_uid.clone();

        let rows = self
            .db_call("db_query", move |conn| {
                let items = match namespace_owned.as_deref() {
                    Some(ns) => {
                        let mut stmt =
                            conn.prepare(queries::OWNERSHIP_INDEXED_NAMESPACED_BY_KIND_AV_UID)?;
                        let rows =
                            stmt.query_map([&kind, ns, &api_version, &owner_uid], |row| {
                                let data_bytes: Vec<u8> = row.get(7)?;
                                let data: serde_json::Value = serde_json::from_slice(&data_bytes)
                                    .map_err(|e| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(e))
                                })?;
                                Ok(super::Resource {
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
                        let mut items = Vec::new();
                        for row in rows {
                            items.push(row?);
                        }
                        items
                    }
                    None => {
                        let mut stmt =
                            conn.prepare(queries::OWNERSHIP_INDEXED_CLUSTER_BY_KIND_AV_UID)?;
                        let rows = stmt.query_map([&kind, &api_version, &owner_uid], |row| {
                            let data_bytes: Vec<u8> = row.get(6)?;
                            let data: serde_json::Value = serde_json::from_slice(&data_bytes)
                                .map_err(|e| {
                                    rusqlite::Error::ToSqlConversionFailure(Box::new(e))
                                })?;
                            Ok(super::Resource {
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
                        let mut items = Vec::new();
                        for row in rows {
                            items.push(row?);
                        }
                        items
                    }
                };
                Ok(items)
            })
            .await?;

        // Defensive filter: confirm owner_uid is actually in ownerReferences.
        let owner_uid_ref = owner_uid_for_filter.as_str();
        let filtered = rows
            .into_iter()
            .filter(|r| {
                r.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|v| v.as_array())
                    .map(|refs| {
                        refs.iter()
                            .any(|o| o.get("uid").and_then(|u| u.as_str()) == Some(owner_uid_ref))
                    })
                    .unwrap_or(false)
            })
            .collect();

        Ok(filtered)
    }

    /// Find namespaced resources that have an ownerReference with uid=="" AND
    /// matching apiVersion + kind + name. Handles the K8s conformance test
    /// pattern where circular ownerRefs use empty UIDs.
    ///
    /// `owner_api_version` is part of the match so two owners from different
    /// API groups with the same kind/name don't collide. Pass an empty
    /// string to match any apiVersion (legacy behavior, used by callers
    /// that don't yet know the parent's apiVersion).
    pub async fn find_owned_by_name_kind_empty_uid(
        &self,
        owner_api_version: &str,
        owner_name: &str,
        owner_kind: &str,
        namespace: Option<&str>,
    ) -> Result<Vec<Resource>> {
        let owner_api_version = owner_api_version.to_string();
        let owner_name = owner_name.to_string();
        let owner_kind = owner_kind.to_string();
        let namespace_owned = namespace.map(str::to_string);

        let owner_api_version_for_filter = owner_api_version.clone();
        let owner_name_for_filter = owner_name.clone();
        let owner_kind_for_filter = owner_kind.clone();

        let namespaced = self
            .db_call("db_query", {
                let namespace = namespace_owned.clone();
                let owner_api_version = owner_api_version.clone();
                let owner_name = owner_name.clone();
                let owner_kind = owner_kind.clone();
                move |conn| {
                    let mut query =
                        queries::OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY.to_string();
                    let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                        vec![Box::new(owner_kind), Box::new(owner_name)];

                    if let Some(ref ns) = namespace {
                        query.push_str(&format!(" AND o.namespace = ?{}", params.len() + 1));
                        params.push(Box::new(ns.clone()));
                    }
                    if !owner_api_version.is_empty() {
                        query
                            .push_str(&format!(" AND o.owner_api_version = ?{}", params.len() + 1));
                        params.push(Box::new(owner_api_version.clone()));
                    }

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
                    let mut items = Vec::new();
                    for row in rows {
                        items.push(row?);
                    }
                    Ok(items)
                }
            })
            .await?;

        // Precise filter: uid must be "" AND name+kind must match in ownerReferences
        let filtered: Vec<Resource> = namespaced
            .into_iter()
            .filter(|item| {
                item.data
                    .pointer("/metadata/ownerReferences")
                    .and_then(|r| r.as_array())
                    .map(|refs| {
                        refs.iter().any(|r| {
                            let uid = r.get("uid").and_then(|u| u.as_str()).unwrap_or("x");
                            let name = r.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let kind = r.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                            let api_version =
                                r.get("apiVersion").and_then(|a| a.as_str()).unwrap_or("");
                            // apiVersion match: empty filter matches any
                            // (legacy callers); otherwise exact match.
                            // Without this two owners from different
                            // groups with the same kind/name would collide
                            // and one's children would be misattributed
                            // to the other.
                            let api_ok = owner_api_version_for_filter.is_empty()
                                || api_version == owner_api_version_for_filter;
                            uid.is_empty()
                                && name == owner_name_for_filter
                                && kind == owner_kind_for_filter
                                && api_ok
                        })
                    })
                    .unwrap_or(false)
            })
            .collect();

        Ok(filtered)
    }
}

#[cfg(test)]
mod owner_index_tests {
    use crate::datastore::test_support::in_memory;
    use serde_json::json;

    /// Asserts list_resources_by_owner_uid returns exactly the matching owner
    /// even with many same-kind resources in the namespace.
    #[tokio::test]
    async fn test_list_resources_by_owner_uid_lookup_returns_only_matching() {
        let db = in_memory().await;

        // Seed 1000 pods owned by "owner-a"
        for i in 0..1000 {
            let pod = json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": format!("a-{}", i),
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "apps/v1",
                            "kind": "ReplicaSet",
                            "name": "rs-a",
                            "uid": "owner-a",
                            "controller": true
                        }
                    ]
                },
                "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
            });
            db.create_resource("v1", "Pod", Some("default"), &format!("a-{}", i), pod)
                .await
                .expect("seed owner-a pod");
        }

        // Seed 1 pod owned by "owner-b"
        let pod_b = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "b-only",
                "namespace": "default",
                "ownerReferences": [
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "rs-b",
                        "uid": "owner-b",
                        "controller": true
                    }
                ]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.create_resource("v1", "Pod", Some("default"), "b-only", pod_b)
            .await
            .expect("seed owner-b pod");

        let start = std::time::Instant::now();
        let owner_b_pods = db
            .list_resources_by_owner_uid("v1", "Pod", Some("default"), "owner-b")
            .await
            .expect("owner UID lookup");
        let elapsed = start.elapsed();

        assert_eq!(
            owner_b_pods.len(),
            1,
            "must return exactly the one owner-b pod"
        );
        assert_eq!(owner_b_pods[0].name, "b-only");
        // Keep this bounded so owner-reference lookups stay suitable for
        // controller reconcile paths.
        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "owner UID lookup took {:?}; expected < 100 ms",
            elapsed
        );

        // Verify owner-a returns 1000
        let owner_a_pods = db
            .list_resources_by_owner_uid("v1", "Pod", Some("default"), "owner-a")
            .await
            .expect("owner UID lookup owner-a");
        assert_eq!(
            owner_a_pods.len(),
            1000,
            "owner-a should have all 1000 pods"
        );
    }

    /// ReplicaSet isolation: a query for a UID with no children returns empty,
    /// not a partial match against any other owner.
    #[tokio::test]
    async fn test_list_resources_by_owner_uid_unrelated_owner_returns_empty() {
        let db = in_memory().await;

        for i in 0..10 {
            let pod = json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": format!("p-{}", i),
                    "namespace": "default",
                    "ownerReferences": [{
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "rs-real",
                        "uid": "real-uid",
                        "controller": true
                    }]
                },
                "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
            });
            db.create_resource("v1", "Pod", Some("default"), &format!("p-{}", i), pod)
                .await
                .expect("seed pod");
        }

        let unrelated = db
            .list_resources_by_owner_uid("v1", "Pod", Some("default"), "phantom-uid")
            .await
            .expect("owner UID lookup");
        assert!(
            unrelated.is_empty(),
            "unrelated owner UID must return zero pods"
        );
    }

    /// Cluster-scoped variant: pass namespace=None to query cluster_resources.
    #[tokio::test]
    async fn test_list_resources_by_owner_uid_cluster_scoped() {
        let db = in_memory().await;

        // ClusterRoleBinding owned by a ClusterRole UID
        let crb = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {
                "name": "test-binding",
                "ownerReferences": [{
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "name": "test-role",
                    "uid": "cr-uid-1",
                    "controller": true
                }]
            },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "test-role" },
            "subjects": []
        });
        db.create_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "test-binding",
            crb,
        )
        .await
        .expect("seed CRB");

        let owned = db
            .list_resources_by_owner_uid(
                "rbac.authorization.k8s.io/v1",
                "ClusterRoleBinding",
                None,
                "cr-uid-1",
            )
            .await
            .expect("cluster-scoped owner UID lookup");
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].name, "test-binding");
        assert!(
            owned[0].namespace.is_none(),
            "cluster-scoped row has no namespace"
        );
    }

    /// api_version filter: two different API groups with the same Kind name
    /// must not collide. Pods in core/v1 should not match a custom
    /// example.com/v1 Pod.
    #[tokio::test]
    async fn test_list_resources_by_owner_uid_api_version_isolation() {
        let db = in_memory().await;

        let core_pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "core-pod",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "ReplicaSet",
                    "name": "shared-uid-rs",
                    "uid": "shared-uid",
                    "controller": true
                }]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.create_resource("v1", "Pod", Some("default"), "core-pod", core_pod)
            .await
            .expect("seed core pod");

        // Must not match a different api_version even with same kind + uid
        let mismatch = db
            .list_resources_by_owner_uid("example.com/v1", "Pod", Some("default"), "shared-uid")
            .await
            .expect("owner UID lookup");
        assert!(
            mismatch.is_empty(),
            "api_version filter must isolate same-kind resources across API groups"
        );

        let core_match = db
            .list_resources_by_owner_uid("v1", "Pod", Some("default"), "shared-uid")
            .await
            .expect("owner UID lookup");
        assert_eq!(core_match.len(), 1);
    }

    #[tokio::test]
    async fn test_list_resources_by_owner_uid_matches_non_first_owner_reference() {
        let db = in_memory().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "multi-owner",
                "namespace": "default",
                "ownerReferences": [
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "other",
                        "uid": "other-owner"
                    },
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "target",
                        "uid": "target-owner",
                        "controller": true
                    }
                ]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.create_resource("v1", "Pod", Some("default"), "multi-owner", pod)
            .await
            .expect("seed multi-owner pod");

        let owned = db
            .list_resources_by_owner_uid("v1", "Pod", Some("default"), "target-owner")
            .await
            .expect("owner UID lookup");

        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].name, "multi-owner");
    }

    /// Verify the owner_ref_index table is populated for non-first owner
    /// references, proving indexed lookups match without json_each.
    #[tokio::test]
    async fn owner_ref_index_matches_non_first_owner_without_json_each() {
        let db = in_memory().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "multi-ref",
                "namespace": "default",
                "ownerReferences": [
                    {
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "first",
                        "uid": "first-uid"
                    },
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "second",
                        "uid": "second-uid",
                        "controller": true
                    },
                    {
                        "apiVersion": "apps/v1",
                        "kind": "ReplicaSet",
                        "name": "third",
                        "uid": "third-uid"
                    }
                ]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.create_resource("v1", "Pod", Some("default"), "multi-ref", pod)
            .await
            .expect("seed multi-ref pod");

        // Verify all three owners are found via indexed lookup
        let first = db
            .find_owned_resources("first-uid", Some("default"))
            .await
            .expect("first owner lookup");
        assert_eq!(first.len(), 1, "first owner should be found");

        let second = db
            .find_owned_resources("second-uid", Some("default"))
            .await
            .expect("second owner lookup");
        assert_eq!(second.len(), 1, "second owner should be found");

        let third = db
            .find_owned_resources("third-uid", Some("default"))
            .await
            .expect("third owner lookup");
        assert_eq!(
            third.len(),
            1,
            "third owner (non-first, non-controller) should be found"
        );
    }

    /// Owner ref index rows must be updated when the resource changes.
    #[tokio::test]
    async fn owner_ref_index_updates_on_resource_update() {
        let db = in_memory().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "update-test",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "rs-old",
                    "uid": "old-uid"
                }]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        let created = db
            .create_resource("v1", "Pod", Some("default"), "update-test", pod)
            .await
            .expect("create pod");

        // Update ownerReferences to a new UID
        let updated = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "update-test",
                "namespace": "default",
                "uid": created.uid,
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "rs-new",
                    "uid": "new-uid"
                }]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.update_resource(
            "v1",
            "Pod",
            Some("default"),
            "update-test",
            updated,
            created.resource_version,
        )
        .await
        .expect("update pod");

        let old_matches = db
            .find_owned_resources("old-uid", Some("default"))
            .await
            .expect("old owner lookup");
        assert!(
            old_matches.is_empty(),
            "old owner UID should no longer be found after update"
        );

        let new_matches = db
            .find_owned_resources("new-uid", Some("default"))
            .await
            .expect("new owner lookup");
        assert_eq!(new_matches.len(), 1, "new owner UID should be found");
    }

    /// Owner ref index rows must be removed when the resource is deleted.
    #[tokio::test]
    async fn owner_ref_index_deletes_rows_on_resource_delete() {
        let db = in_memory().await;

        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "delete-test",
                "namespace": "default",
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "ReplicaSet",
                    "name": "rs",
                    "uid": "delete-uid"
                }]
            },
            "spec": { "containers": [{ "name": "c", "image": "nginx" }] }
        });
        db.create_resource("v1", "Pod", Some("default"), "delete-test", pod)
            .await
            .expect("create pod");

        let before = db
            .find_owned_resources("delete-uid", Some("default"))
            .await
            .expect("before delete");
        assert_eq!(before.len(), 1);

        db.delete_resource("v1", "Pod", Some("default"), "delete-test")
            .await
            .expect("delete pod");

        let after = db
            .find_owned_resources("delete-uid", Some("default"))
            .await
            .expect("after delete");
        assert!(
            after.is_empty(),
            "owner ref index rows must be removed on delete"
        );
    }

    /// Cluster-scoped owner ref index lookup works correctly.
    #[tokio::test]
    async fn owner_ref_index_cluster_scoped_lookup() {
        let db = in_memory().await;

        let crb = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {
                "name": "idx-test-binding",
                "ownerReferences": [{
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "name": "idx-test-role",
                    "uid": "idx-cr-uid"
                }]
            },
            "roleRef": { "apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "idx-test-role" },
            "subjects": []
        });
        db.create_resource(
            "rbac.authorization.k8s.io/v1",
            "ClusterRoleBinding",
            None,
            "idx-test-binding",
            crb,
        )
        .await
        .expect("seed CRB");

        let owned = db
            .find_owned_resources("idx-cr-uid", None)
            .await
            .expect("cluster-scoped indexed lookup");
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].name, "idx-test-binding");
    }

    /// EXPLAIN QUERY PLAN test: indexed lookup must use the owner_refs index.
    #[test]
    fn explain_query_plan_owner_uid_uses_index() {
        use rusqlite::Connection;
        let mut conn = Connection::open_in_memory().unwrap();
        super::super::schema::init_schema_in_conn(&mut conn).unwrap();

        let mut plan_parts = Vec::new();
        let mut stmt = conn
            .prepare(
                "EXPLAIN QUERY PLAN SELECT r.id FROM namespaced_resources r \
                 INNER JOIN resource_owner_refs o ON o.api_version = r.api_version \
                 AND o.kind = r.kind AND o.namespace = r.namespace AND o.name = r.name \
                 WHERE o.owner_uid = ?",
            )
            .unwrap();
        let rows = stmt
            .query_map(["test-uid"], |row| {
                let detail: String = row.get(3)?;
                Ok(detail)
            })
            .unwrap();
        for row in rows {
            plan_parts.push(row.unwrap());
        }

        let plan = plan_parts.join("; ");
        assert!(
            plan.contains("resource_owner_refs"),
            "query plan must reference resource_owner_refs table, got: {plan}"
        );
    }

    #[test]
    fn owner_ref_schema_has_composite_lookup_indexes() {
        use rusqlite::Connection;
        let mut conn = Connection::open_in_memory().unwrap();
        crate::datastore::sqlite::schema::init_schema_in_conn(&mut conn).unwrap();

        let indexes = conn
            .prepare("PRAGMA index_list('resource_owner_refs')")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_resource_owner_refs_uid"),
            "resource_owner_refs must expose the composite owner_uid lookup index, got {indexes:?}"
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_resource_owner_refs_owner_identity"),
            "resource_owner_refs must expose the owner identity lookup index, got {indexes:?}"
        );
        assert!(
            indexes
                .iter()
                .any(|name| name == "idx_resource_owner_refs_resource"),
            "resource_owner_refs must keep the per-resource cleanup index, got {indexes:?}"
        );
    }

    #[test]
    fn empty_uid_owner_lookup_uses_owner_identity_index() {
        use rusqlite::Connection;
        let mut conn = Connection::open_in_memory().unwrap();
        crate::datastore::sqlite::schema::init_schema_in_conn(&mut conn).unwrap();

        let mut stmt = conn
            .prepare(
                "EXPLAIN QUERY PLAN SELECT r.id \
                 FROM resource_owner_refs o \
                 INNER JOIN namespaced_resources r ON r.api_version = o.api_version \
                   AND r.kind = o.kind AND r.namespace = o.namespace AND r.name = o.name \
                 WHERE o.owner_api_version = ?1 AND o.owner_kind = ?2 \
                   AND o.owner_name = ?3 AND o.namespace = ?4 AND o.owner_uid = ''",
            )
            .unwrap();
        let details = stmt
            .query_map(["apps/v1", "ReplicaSet", "cycle", "default"], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let plan = details.join("; ");

        assert!(
            plan.contains("idx_resource_owner_refs_owner_identity"),
            "empty-UID owner identity lookup must use idx_resource_owner_refs_owner_identity, got: {plan}"
        );
    }

    #[test]
    fn empty_uid_owner_lookup_has_no_json_text_prefilter() {
        let query =
            crate::datastore::sqlite::queries::OWNERSHIP_INDEXED_NAMESPACED_EMPTY_UID_BY_IDENTITY;

        assert!(
            query.contains("resource_owner_refs"),
            "empty UID ownership lookup must be driven by resource_owner_refs"
        );
        assert!(
            !query.contains("INSTR(CAST(r.data AS TEXT)"),
            "empty UID ownership lookup must be driven by resource_owner_refs, not a JSON text prefilter"
        );
        assert!(
            !query.contains("json_each"),
            "empty UID ownership lookup must not scan ownerReferences JSON"
        );
    }
}
