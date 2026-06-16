use super::*;
use std::net::Ipv4Addr;

fn sample_row(uid: &str, pod_ip: Ipv4Addr, mode: PodEndpointMode) -> PodEndpointRow {
    PodEndpointRow {
        pod_uid: uid.to_string(),
        namespace: "default".to_string(),
        pod_name: format!("pod-{uid}"),
        node_name: "node-a".to_string(),
        mode,
        pod_ip,
        node_ip: pod_ip,
        host_port_tcp: None,
        host_port_udp: None,
        generation: 1,
        updated_at: 1_700_000_000,
    }
}

#[tokio::test]
async fn test_pod_endpoints_create_round_trip() {
    let db = Datastore::new_in_memory().await.unwrap();
    let row = sample_row(
        "uid-rt",
        Ipv4Addr::new(10, 42, 1, 5),
        PodEndpointMode::EncryptedDirect,
    );
    db.pod_endpoint_upsert(row.clone()).await.unwrap();

    let fetched = db
        .pod_endpoint_get_by_pod_ip(Ipv4Addr::new(10, 42, 1, 5))
        .await
        .unwrap()
        .expect("row should be present after upsert");
    assert_eq!(fetched, row, "round-trip row must equal upsert input");
}

#[tokio::test]
async fn test_pod_endpoints_unique_pod_uid_violation() {
    // Two distinct pods inserted, then a raw INSERT (no REPLACE) of a third
    // row reusing the first pod's uid must violate the PRIMARY KEY constraint.
    let db = Datastore::new_in_memory().await.unwrap();
    let a = sample_row(
        "uid-a",
        Ipv4Addr::new(10, 42, 0, 1),
        PodEndpointMode::EncryptedDirect,
    );
    let b = sample_row(
        "uid-b",
        Ipv4Addr::new(10, 42, 0, 2),
        PodEndpointMode::EncryptedDirect,
    );
    db.pod_endpoint_upsert(a.clone()).await.unwrap();
    db.pod_endpoint_upsert(b.clone()).await.unwrap();

    let conflict = db
        .db_call("test_pod_endpoints_dup_insert", move |conn| {
            conn.execute(
                "INSERT INTO pod_endpoints \
                 (pod_uid, namespace, pod_name, node_name, mode, pod_ip, generation, updated_at) \
                 VALUES (?1, 'default', 'pod-dup', 'node-a', 'encrypted_direct', '10.42.0.99', 1, 1700000000)",
                rusqlite::params!["uid-a"],
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await;
    assert!(
        conflict.is_err(),
        "duplicate pod_uid INSERT must violate PRIMARY KEY"
    );

    // After the failed insert, original two rows are still there.
    let rows = db.pod_endpoint_list_by_node("node-a").await.unwrap();
    assert_eq!(rows.len(), 2, "exactly two rows after failed dup insert");
}

#[tokio::test]
async fn test_pod_endpoints_list_by_node() {
    let db = Datastore::new_in_memory().await.unwrap();
    let mut a = sample_row(
        "uid-1",
        Ipv4Addr::new(10, 42, 1, 10),
        PodEndpointMode::EncryptedDirect,
    );
    let mut b = sample_row(
        "uid-2",
        Ipv4Addr::new(10, 42, 1, 11),
        PodEndpointMode::EncryptedDirect,
    );
    let mut c = sample_row(
        "uid-3",
        Ipv4Addr::new(10, 42, 2, 10),
        PodEndpointMode::Hostport,
    );
    a.node_name = "node-a".to_string();
    b.node_name = "node-a".to_string();
    c.node_name = "node-b".to_string();
    c.host_port_tcp = Some(31000);
    c.host_port_udp = Some(31000);
    db.pod_endpoint_upsert(a).await.unwrap();
    db.pod_endpoint_upsert(b).await.unwrap();
    db.pod_endpoint_upsert(c).await.unwrap();

    let on_a = db.pod_endpoint_list_by_node("node-a").await.unwrap();
    assert_eq!(on_a.len(), 2);
    assert!(on_a.iter().all(|r| r.node_name == "node-a"));

    let on_b = db.pod_endpoint_list_by_node("node-b").await.unwrap();
    assert_eq!(on_b.len(), 1);
    assert_eq!(on_b[0].pod_uid, "uid-3");
    assert_eq!(on_b[0].host_port_tcp, Some(31000));

    let on_other = db.pod_endpoint_list_by_node("node-zz").await.unwrap();
    assert!(on_other.is_empty());
}

#[tokio::test]
async fn test_pod_endpoints_list_all_orders_by_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let b = sample_row(
        "uid-b",
        Ipv4Addr::new(10, 42, 0, 2),
        PodEndpointMode::EncryptedDirect,
    );
    let a = sample_row(
        "uid-a",
        Ipv4Addr::new(10, 42, 0, 1),
        PodEndpointMode::Hostport,
    );
    db.pod_endpoint_upsert(b).await.unwrap();
    db.pod_endpoint_upsert(a).await.unwrap();

    let rows = db.pod_endpoint_list_all().await.unwrap();
    let uids: Vec<_> = rows.into_iter().map(|row| row.pod_uid).collect();
    assert_eq!(uids, vec!["uid-a", "uid-b"]);
}

#[tokio::test]
async fn test_pod_endpoints_watch_emits_create_update_delete() {
    let db = Datastore::new_in_memory().await.unwrap();
    let mut rx = db.subscribe_pod_endpoints();

    // CREATE
    let mut row = sample_row(
        "uid-w",
        Ipv4Addr::new(10, 42, 9, 1),
        PodEndpointMode::EncryptedDirect,
    );
    db.pod_endpoint_upsert(row.clone()).await.unwrap();
    let evt = rx.recv().await.expect("create event");
    match evt {
        PodEndpointEvent::Upsert(r) => assert_eq!(r, row),
        other => panic!("expected Upsert, got {other:?}"),
    }

    // UPDATE (same uid, new generation/pod_ip)
    row.generation = 2;
    row.pod_ip = Ipv4Addr::new(10, 42, 9, 2);
    db.pod_endpoint_upsert(row.clone()).await.unwrap();
    let evt = rx.recv().await.expect("update old-ip delete event");
    match evt {
        PodEndpointEvent::Delete { pod_uid, pod_ip } => {
            assert_eq!(pod_uid, "uid-w");
            assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 9, 1));
        }
        other => panic!("expected Delete for replaced pod IP, got {other:?}"),
    }
    let evt = rx.recv().await.expect("update event");
    match evt {
        PodEndpointEvent::Upsert(r) => assert_eq!(r.generation, 2),
        other => panic!("expected Upsert (update), got {other:?}"),
    }

    // DELETE
    db.pod_endpoint_delete_by_uid("uid-w").await.unwrap();
    let evt = rx.recv().await.expect("delete event");
    match evt {
        PodEndpointEvent::Delete { pod_uid, pod_ip } => {
            assert_eq!(pod_uid, "uid-w");
            assert_eq!(pod_ip, Ipv4Addr::new(10, 42, 9, 2));
        }
        other => panic!("expected Delete, got {other:?}"),
    }
}

#[tokio::test]
async fn test_schema_init_includes_pod_endpoints() {
    // Fresh node-local DB must have the pod_endpoints table after init_schema().
    let db = Datastore::new_in_memory().await.unwrap();
    let exists: i64 = db
        .node_db_call("test_schema_pod_endpoints_present", |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='pod_endpoints'",
                [],
                |row| row.get(0),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(
        exists, 1,
        "pod_endpoints table must exist after schema init"
    );

    let node_idx: i64 = db
        .node_db_call("test_schema_pod_endpoints_node_idx", |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='pod_endpoints_node'",
                [],
                |row| row.get(0),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(node_idx, 1, "pod_endpoints_node index must exist");

    let ns_pod_idx: i64 = db
        .node_db_call("test_schema_pod_endpoints_ns_pod_idx", |conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='pod_endpoints_ns_pod'",
                [],
                |row| row.get(0),
            )
            .map_err(tokio_rusqlite::Error::from)
        })
        .await
        .unwrap();
    assert_eq!(ns_pod_idx, 1, "pod_endpoints_ns_pod index must exist");
}
