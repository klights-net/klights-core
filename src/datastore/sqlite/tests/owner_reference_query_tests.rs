use super::*;
use crate::pod_identity::PodIdentity;
use serde_json::json;
#[test]
fn test_resolve_field_path_top_level() {
    let data = json!({"reason": "Started", "type": "Normal"});
    assert_eq!(
        resolve_field_path(&data, "reason").as_deref(),
        Some("Started")
    );
    assert_eq!(resolve_field_path(&data, "type").as_deref(), Some("Normal"));
}

#[test]
fn test_resolve_field_path_nested() {
    let data = json!({"involvedObject": {"name": "my-pod", "uid": "abc-123"}});
    assert_eq!(
        resolve_field_path(&data, "involvedObject.name").as_deref(),
        Some("my-pod")
    );
    assert_eq!(
        resolve_field_path(&data, "involvedObject.uid").as_deref(),
        Some("abc-123")
    );
}

#[test]
fn test_resolve_field_path_missing_returns_none() {
    let data = json!({"metadata": {"name": "test"}});
    assert_eq!(resolve_field_path(&data, "involvedObject.name"), None);
    assert_eq!(resolve_field_path(&data, "nonexistent"), None);
}

#[test]
fn test_resolve_field_path_boolean() {
    let data = json!({"spec": {"unschedulable": false}});
    assert_eq!(
        resolve_field_path(&data, "spec.unschedulable").as_deref(),
        Some("false")
    );
    let data2 = json!({"spec": {"unschedulable": true}});
    assert_eq!(
        resolve_field_path(&data2, "spec.unschedulable").as_deref(),
        Some("true")
    );
}

#[test]
fn test_filter_by_field_selector_involvedobject_name_filters_correctly() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event1".to_string(),
            uid: "uid-event-a-1".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a", "kind": "Pod"},
                "reason": "Started",
                "message": "Started container"
            })),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-b.event1".to_string(),
            uid: "uid-event-b-1".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-b", "uid": "uid-b", "kind": "Pod"},
                "reason": "Pulling",
                "message": "Pulling image"
            })),
        },
    ];

    let filtered = filter_by_field_selector(items, "involvedObject.name=pod-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a.event1");
}

#[test]
fn test_filter_by_field_selector_multiple_conditions_all_applied() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event1".to_string(),
            uid: "uid-event-a-1".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a", "kind": "Pod"},
                "reason": "Started"
            })),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a.event2".to_string(),
            uid: "uid-event-a-2".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({
                "involvedObject": {"name": "pod-a", "uid": "uid-a-different", "kind": "Pod"},
                "reason": "Failed"
            })),
        },
    ];

    // Both conditions must match
    let filtered =
        filter_by_field_selector(items, "involvedObject.name=pod-a,involvedObject.uid=uid-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a.event1");
}

#[tokio::test]
async fn test_list_resources_with_field_selector_filters_events() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create two events for different pods
    let event_a = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "pod-a.evt1", "namespace": "default"},
        "involvedObject": {"name": "pod-a", "namespace": "default", "uid": "uid-a", "kind": "Pod"},
        "reason": "Started",
        "message": "Started container in pod-a"
    });
    db.create_resource("v1", "Event", Some("default"), "pod-a.evt1", event_a)
        .await
        .unwrap();

    let event_b = json!({
        "apiVersion": "v1",
        "kind": "Event",
        "metadata": {"name": "pod-b.evt1", "namespace": "default"},
        "involvedObject": {"name": "pod-b", "namespace": "default", "uid": "uid-b", "kind": "Pod"},
        "reason": "Pulling",
        "message": "Pulling image for pod-b"
    });
    db.create_resource("v1", "Event", Some("default"), "pod-b.evt1", event_b)
        .await
        .unwrap();

    // Without field selector: returns all events
    let all = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();
    assert_eq!(all.items.len(), 2, "Without selector, both events returned");

    // With field selector: only pod-a events
    let filtered = db
        .list_resources(
            "v1",
            "Event",
            Some("default"),
            crate::datastore::ResourceListQuery::new(
                None,
                Some("involvedObject.name=pod-a"),
                None,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        filtered.items.len(),
        1,
        "Field selector should filter to pod-a events only"
    );
    assert_eq!(filtered.items[0].name, "pod-a.evt1");
}

#[test]
fn test_filter_by_field_selector_inequality() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "evt-normal".to_string(),
            uid: "uid-event-normal".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(json!({"type": "Normal"})),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Event".to_string(),
            namespace: Some("default".to_string()),
            name: "evt-warning".to_string(),
            uid: "uid-event-warning".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(json!({"type": "Warning"})),
        },
    ];

    let filtered = filter_by_field_selector(items, "type!=Normal");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "evt-warning");
}

#[test]
fn test_filter_by_field_selector_metadata_name() {
    let items = vec![
        Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-a".to_string(),
            uid: "uid-pod-a".to_string(),
            resource_version: 1,
            data: std::sync::Arc::new(
                json!({"metadata": {"name": "pod-a"}, "status": {"phase": "Running"}}),
            ),
        },
        Resource {
            id: 1,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "pod-b".to_string(),
            uid: "uid-pod-b".to_string(),
            resource_version: 2,
            data: std::sync::Arc::new(
                json!({"metadata": {"name": "pod-b"}, "status": {"phase": "Pending"}}),
            ),
        },
    ];

    let filtered = filter_by_field_selector(items.clone(), "metadata.name=pod-a");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a");

    let filtered = filter_by_field_selector(items, "status.phase=Running");
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].name, "pod-a");
}

#[test]
fn test_filter_by_field_selector_empty_returns_all() {
    let items = vec![Resource {
        id: 0,
        api_version: "v1".to_string(),
        kind: "Pod".to_string(),
        namespace: Some("default".to_string()),
        name: "test".to_string(),
        uid: "uid-test".to_string(),
        resource_version: 1,
        data: std::sync::Arc::new(json!({})),
    }];
    let filtered = filter_by_field_selector(items, "");
    assert_eq!(filtered.len(), 1);
}

// ========================
// pod_sandboxes table tests
// ========================

#[tokio::test]
async fn test_record_sandbox_then_get_returns_id() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-abc")
        .await
        .unwrap();
    let id = db.get_sandbox("default", "mypod").await.unwrap();
    assert_eq!(id, Some("sandbox-abc".to_string()));
}

#[tokio::test]
async fn test_get_sandbox_returns_none_when_not_recorded() {
    let db = Datastore::new_in_memory().await.unwrap();
    let id = db.get_sandbox("default", "no-such-pod").await.unwrap();
    assert_eq!(id, None);
}

#[tokio::test]
async fn test_delete_sandbox_removes_row() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-abc")
        .await
        .unwrap();
    db.delete_sandbox("default", "mypod").await.unwrap();
    let id = db.get_sandbox("default", "mypod").await.unwrap();
    assert_eq!(id, None);
}

#[tokio::test]
async fn pod_runtime_primary_key_is_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let pk_ordinals = db
        .node_db_call("test_pod_runtime_pk_info", move |conn| {
            let mut stmt = conn.prepare("PRAGMA table_info(pod_runtime)")?;
            let mut rows = stmt.query([])?;
            let mut ordinals = std::collections::BTreeMap::new();
            while let Some(row) = rows.next()? {
                let name: String = row.get(1)?;
                let pk: i64 = row.get(5)?;
                ordinals.insert(name, pk);
            }
            Ok(ordinals)
        })
        .await
        .unwrap();

    assert_eq!(pk_ordinals.get("pod_uid"), Some(&1));
    assert_eq!(pk_ordinals.get("namespace"), Some(&0));
    assert_eq!(pk_ordinals.get("pod_name"), Some(&0));
}

#[tokio::test]
async fn concurrent_sandbox_insert_different_uids_both_survive() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-old")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-456", "sandbox-new")
        .await
        .unwrap();

    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-123")
            .await
            .unwrap(),
        Some("sandbox-old".to_string())
    );
    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-456")
            .await
            .unwrap(),
        Some("sandbox-new".to_string())
    );

    let sandboxes = db.list_sandboxes().await.unwrap();
    assert_eq!(sandboxes.len(), 2);
    assert!(sandboxes.iter().any(|row| {
        row.namespace == "default"
            && row.pod_name == "mypod"
            && row.pod_uid == "uid-123"
            && row.sandbox_id == "sandbox-old"
    }));
    assert!(sandboxes.iter().any(|row| {
        row.namespace == "default"
            && row.pod_name == "mypod"
            && row.pod_uid == "uid-456"
            && row.sandbox_id == "sandbox-new"
    }));
}

#[tokio::test]
async fn record_sandbox_same_uid_replaces_same_uid_only() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-old")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-456", "sandbox-other")
        .await
        .unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-new")
        .await
        .unwrap();

    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-123")
            .await
            .unwrap(),
        Some("sandbox-new".to_string())
    );
    assert_eq!(
        db.get_sandbox_for_uid("default", "mypod", "uid-456")
            .await
            .unwrap(),
        Some("sandbox-other".to_string())
    );
}

#[tokio::test]
async fn test_get_sandbox_for_uid_matches_exact_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-123", "sandbox-abc")
        .await
        .unwrap();

    let id = db
        .get_sandbox_for_uid("default", "mypod", "uid-123")
        .await
        .unwrap();
    assert_eq!(id, Some("sandbox-abc".to_string()));
}

#[tokio::test]
async fn test_get_sandbox_for_uid_returns_none_on_uid_mismatch() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-old", "sandbox-old")
        .await
        .unwrap();

    let id = db
        .get_sandbox_for_uid("default", "mypod", "uid-new")
        .await
        .unwrap();
    assert_eq!(id, None);
}

#[tokio::test]
async fn test_delete_sandbox_for_uid_does_not_remove_replacement_row() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_sandbox("default", "mypod", "uid-new", "sandbox-new")
        .await
        .unwrap();

    db.delete_sandbox_for_uid("default", "mypod", "uid-old", "sandbox-old")
        .await
        .unwrap();

    let id = db.get_sandbox("default", "mypod").await.unwrap();
    assert_eq!(id, Some("sandbox-new".to_string()));
}

#[tokio::test]
async fn test_ipam_allocate_wraps_to_first_free_gap() {
    let db = Datastore::new_in_memory().await.unwrap();
    let subnet_base = PodSubnet::parse("10.77.0.0/29").unwrap().base();
    let subnet_size = 8;

    db.record_pod_network(
        "sandbox-a",
        &PodIdentity::new("default", "pod-a", "uid-a"),
        "10.77.0.2",
        subnet_base + 2,
        "vetha",
        "/run/netns/a",
    )
    .await
    .unwrap();
    db.record_pod_network(
        "sandbox-last",
        &PodIdentity::new("default", "pod-last", "uid-last"),
        "10.77.0.6",
        subnet_base + 6,
        "vethlast",
        "/run/netns/last",
    )
    .await
    .unwrap();

    let (ip, ip_int) = db.ipam_allocate(subnet_base, subnet_size).await.unwrap();
    assert_eq!(ip, "10.77.0.3");
    assert_eq!(ip_int, subnet_base + 3);
}

#[tokio::test]
async fn test_record_pod_network_rejects_ip_conflict_without_replacing_existing() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_pod_network(
        "sandbox-a",
        &PodIdentity::new("default", "pod-a", "uid-a"),
        "10.77.0.2",
        0x0a4d0002,
        "vetha",
        "/run/netns/a",
    )
    .await
    .unwrap();

    let conflict = db
        .record_pod_network(
            "sandbox-b",
            &PodIdentity::new("default", "pod-b", "uid-b"),
            "10.77.0.2",
            0x0a4d0002,
            "vethb",
            "/run/netns/b",
        )
        .await;

    assert!(conflict.is_err(), "duplicate pod IP must fail");
    assert_eq!(
        db.get_pod_network("sandbox-a").await.unwrap(),
        Some(crate::datastore::PodNetworkEndpoint {
            ip_addr: "10.77.0.2".to_string(),
            veth_host: "vetha".to_string(),
            netns_path: "/run/netns/a".to_string(),
        })
    );
    assert_eq!(db.get_pod_network("sandbox-b").await.unwrap(), None);
}

#[tokio::test]
async fn test_get_pod_network_by_pod_identity_returns_active_allocation() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.record_pod_network(
        "cni-container-id",
        &PodIdentity::new("default", "pod-a", "uid-a"),
        "10.77.0.9",
        0x0a4d0009,
        "vetha",
        "/run/netns/a",
    )
    .await
    .unwrap();

    let endpoint = db
        .get_pod_network_for_pod("default", "pod-a", "uid-a")
        .await
        .unwrap();

    assert_eq!(
        endpoint,
        Some(crate::datastore::PodNetworkEndpoint {
            ip_addr: "10.77.0.9".to_string(),
            veth_host: "vetha".to_string(),
            netns_path: "/run/netns/a".to_string(),
        })
    );
    assert_eq!(
        db.get_pod_network_for_pod("default", "pod-a", "other-uid")
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn test_ipam_allocate_and_record_is_idempotent_for_same_sandbox() {
    let db = Datastore::new_in_memory().await.unwrap();
    let subnet_base = PodSubnet::parse("10.88.0.0/24").unwrap().base();
    let subnet_size = 1u32 << 8;

    let first = db
        .ipam_allocate_and_record_pod_network(
            "sandbox-idem",
            &PodIdentity::new("default", "pod-idem", "uid-idem"),
            subnet_base,
            subnet_size,
            "vethidem",
            "/run/netns/idem",
        )
        .await
        .unwrap();
    let second = db
        .ipam_allocate_and_record_pod_network(
            "sandbox-idem",
            &PodIdentity::new("default", "pod-idem", "uid-idem"),
            subnet_base,
            subnet_size,
            "vethidem",
            "/run/netns/idem",
        )
        .await
        .unwrap();

    assert_eq!(first, second, "same sandbox must keep same reserved IP");
    assert_eq!(
        db.get_pod_network("sandbox-idem").await.unwrap(),
        Some(crate::datastore::PodNetworkEndpoint {
            ip_addr: first.0.clone(),
            veth_host: "vethidem".to_string(),
            netns_path: "/run/netns/idem".to_string(),
        })
    );
}

#[tokio::test]
async fn test_ipam_allocate_and_record_skips_used_ips() {
    let db = Datastore::new_in_memory().await.unwrap();
    let subnet_base = PodSubnet::parse("10.89.0.0/29").unwrap().base();
    let subnet_size = 8;

    db.record_pod_network(
        "sandbox-a",
        &PodIdentity::new("default", "pod-a", "uid-a"),
        "10.89.0.2",
        subnet_base + 2,
        "vetha",
        "/run/netns/a",
    )
    .await
    .unwrap();
    db.record_pod_network(
        "sandbox-b",
        &PodIdentity::new("default", "pod-b", "uid-b"),
        "10.89.0.3",
        subnet_base + 3,
        "vethb",
        "/run/netns/b",
    )
    .await
    .unwrap();

    let (ip, ip_int) = db
        .ipam_allocate_and_record_pod_network(
            "sandbox-c",
            &PodIdentity::new("default", "pod-c", "uid-c"),
            subnet_base,
            subnet_size,
            "vethc",
            "/run/netns/c",
        )
        .await
        .unwrap();

    assert_eq!(ip, "10.89.0.4");
    assert_eq!(ip_int, subnet_base + 4);
    assert_eq!(
        db.get_pod_network("sandbox-c").await.unwrap(),
        Some(crate::datastore::PodNetworkEndpoint {
            ip_addr: "10.89.0.4".to_string(),
            veth_host: "vethc".to_string(),
            netns_path: "/run/netns/c".to_string(),
        })
    );
}

#[tokio::test]
async fn test_list_pod_network_sandbox_ids_returns_active_allocations() {
    let db = Datastore::new_in_memory().await.unwrap();
    let subnet_base = PodSubnet::parse("10.90.0.0/29").unwrap().base();

    db.record_pod_network(
        "sandbox-a",
        &PodIdentity::new("default", "pod-a", "uid-a"),
        "10.90.0.2",
        subnet_base + 2,
        "vetha",
        "/run/netns/a",
    )
    .await
    .unwrap();
    db.record_pod_network(
        "sandbox-b",
        &PodIdentity::new("default", "pod-b", "uid-b"),
        "10.90.0.3",
        subnet_base + 3,
        "vethb",
        "/run/netns/b",
    )
    .await
    .unwrap();

    let mut ids = db.list_pod_network_sandbox_ids().await.unwrap();
    ids.sort();
    assert_eq!(ids, vec!["sandbox-a".to_string(), "sandbox-b".to_string()]);
}
