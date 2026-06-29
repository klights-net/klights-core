use super::*;
use crate::datastore::command::StorageCommand;
use crate::datastore::sqlite::BuildOutboxOutcome;
use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
use crate::log_apply::LogApplyMutation;
use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};
use serde_json::json;
use std::net::Ipv4Addr;

use crate::log_apply::{
    LogApplyCommit, LogApplyNodeDataplaneRow, LogApplyNodeSubnetAllocation, LogApplyNodeSubnetRow,
};

type NodeSubnetReplayRow = (
    String,
    String,
    i64,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    i64,
);

type NodeDataplaneReplayRow = (
    String,
    String,
    String,
    Option<String>,
    String,
    Option<i64>,
    i64,
);

async fn select_node_subnet_rows(db: &Datastore) -> Vec<NodeSubnetReplayRow> {
    db.db_call("test_select_node_subnet_rows", |conn| {
        let mut stmt = conn.prepare(
            "SELECT node_name, subnet, subnet_base_int, vtep_ip, vtep_mac, node_ip, mode, \
             hostport_range, created_at FROM node_subnets ORDER BY node_name",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .await
    .unwrap()
}

async fn select_node_dataplane_rows(db: &Datastore) -> Vec<NodeDataplaneReplayRow> {
    db.db_call("test_select_node_dataplane_rows", |conn| {
        let mut stmt = conn.prepare(
            "SELECT node_name, mode, encryption, public_key, endpoint, port, updated_at \
         FROM node_dataplane ORDER BY node_name",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn test_node_dataplane_metadata_round_trips_enabled_and_disabled() {
    let db = Datastore::new_in_memory().await.unwrap();
    let public_key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string();

    let enabled = DataplanePeerMetadata::try_new(
        "node-a".to_string(),
        DataplaneMode::Root,
        DataplaneEncryption::Enabled,
        Some(public_key.clone()),
        Some("192.0.2.10".to_string()),
        Some(51_820),
    )
    .unwrap();
    db.update_node_dataplane(enabled.clone()).await.unwrap();
    assert_eq!(
        db.get_node_dataplane("node-a").await.unwrap(),
        Some(enabled)
    );

    let disabled = DataplanePeerMetadata::try_new(
        "node-a".to_string(),
        DataplaneMode::Rootless,
        DataplaneEncryption::Disabled,
        Some(public_key),
        Some("192.0.2.11".to_string()),
        Some(0),
    )
    .unwrap();
    db.update_node_dataplane(disabled.clone()).await.unwrap();
    let stored = db.get_node_dataplane("node-a").await.unwrap().unwrap();
    assert_eq!(stored, disabled);
    assert!(stored.public_key.is_none());
}

#[tokio::test]
async fn test_node_peer_metadata_uses_deterministic_storage_timestamps() {
    let db = Datastore::new_in_memory().await.unwrap();
    db.allocate_node_subnet("node-a", "10.42.0.0/16", "192.0.2.10")
        .await
        .unwrap();
    db.update_node_dataplane(
        DataplanePeerMetadata::try_new(
            "node-a".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Enabled,
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            Some("192.0.2.10".to_string()),
            Some(51_820),
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let (created_at, updated_at): (i64, i64) = db
        .db_call("test_node_peer_metadata_storage_timestamps", |conn| {
            let created_at = conn.query_row(
                "SELECT created_at FROM node_subnets WHERE node_name = 'node-a'",
                [],
                |row| row.get(0),
            )?;
            let updated_at = conn.query_row(
                "SELECT updated_at FROM node_dataplane WHERE node_name = 'node-a'",
                [],
                |row| row.get(0),
            )?;
            Ok((created_at, updated_at))
        })
        .await
        .unwrap();

    assert_eq!(
        (created_at, updated_at),
        (0, 0),
        "replicated peer metadata must not diverge on local wall-clock storage timestamps"
    );
}

#[tokio::test]
async fn build_update_node_dataplane_log_apply_does_not_mutate_leader_node() {
    let db = Datastore::new_in_memory().await.unwrap();
    let node = db
        .create_resource(
            "v1",
            "Node",
            None,
            "node-a",
            json!({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "node-a"},
                "spec": {},
                "status": {}
            }),
        )
        .await
        .unwrap();
    db.allocate_node_subnet("node-a", "10.50.0.0/16", "10.99.0.10")
        .await
        .unwrap();
    let before_watch_count = db.count_watch_events().await.unwrap();

    let command = StorageCommand::UpdateNodeDataplane {
        node_name: "node-a".to_string(),
        mode: "root".to_string(),
        encryption: "enabled".to_string(),
        public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        endpoint: "10.99.0.10".to_string(),
        port: Some(7679),
    };
    let payload = OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "dataplane-node-a",
            OutboxOperation::NodeDataplane.as_str(),
            payload.as_ref(),
            "node-a",
        )
        .await
        .unwrap();
    let BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected dataplane command to need raft proposal");
    };

    let leader_node_after_build = db
        .get_resource("v1", "Node", None, "node-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        leader_node_after_build.resource_version, node.resource_version,
        "building the raft payload must not locally update the leader's Node resource"
    );
    assert_eq!(
        db.count_watch_events().await.unwrap(),
        before_watch_count,
        "building the raft payload must not locally emit watch history"
    );
    assert!(
        commit.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogApplyMutation::PutNodeDataplane(row) if row.node_name == "node-a"
            )
        }),
        "commit must still persist node_dataplane"
    );
    assert!(
        commit.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                LogApplyMutation::PutResource(row)
                    if row.api_version == "v1" && row.kind == "Node" && row.name == "node-a"
            )
        }),
        "Node routing metadata must be carried inside the raft-applied commit"
    );

    db.apply_log_apply_commit(commit).await.unwrap();
    let applied_node = db
        .get_resource("v1", "Node", None, "node-a")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(applied_node.data["spec"]["podCIDR"], json!("10.50.0.0/24"));
    assert_eq!(
        applied_node.data["metadata"]["annotations"]["klights.io/dataplane-endpoint"],
        json!("10.99.0.10")
    );
    let addresses = applied_node
        .data
        .pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .expect("NodeDataplane apply should preserve status.addresses");
    assert!(
        addresses.iter().any(|address| {
            address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP")
                && address.get("address").and_then(|value| value.as_str()) == Some("10.99.0.10")
        }),
        "raft-applied NodeDataplane metadata must publish the observed ExternalIP: {addresses:?}",
    );
}

#[tokio::test]
async fn node_registration_outbox_uses_dataplane_annotation_for_external_ip() {
    let db = Datastore::new_in_memory().await.unwrap();
    let command = StorageCommand::CreateResource {
        api_version: "v1".to_string(),
        kind: "Node".to_string(),
        namespace: None,
        name: "node-a".to_string(),
        data: json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": "node-a",
                "annotations": {
                    "klights.io/dataplane-endpoint": "10.99.0.11"
                }
            },
            "status": {
                "addresses": [
                    {"type": "Hostname", "address": "node-a"},
                    {"type": "InternalIP", "address": "172.31.11.2"}
                ]
            }
        }),
    };
    let payload = OutboxPayload::from_command(command)
        .encode_protobuf()
        .unwrap();
    let outcome = db
        .build_log_apply_commit_for_outbox(
            "node-registration-node-a",
            OutboxOperation::NodeRegistration.as_str(),
            payload.as_ref(),
            "node-a",
        )
        .await
        .unwrap();
    let BuildOutboxOutcome::NeedsPropose { commit, .. } = outcome else {
        panic!("expected node registration command to need raft proposal");
    };

    db.apply_log_apply_commit(commit).await.unwrap();

    let node = db
        .get_resource("v1", "Node", None, "node-a")
        .await
        .unwrap()
        .expect("NodeRegistration should create Node");
    let addresses = node
        .data
        .pointer("/status/addresses")
        .and_then(|value| value.as_array())
        .expect("Node should have status.addresses");
    assert!(addresses.iter().any(|address| {
        address.get("type").and_then(|value| value.as_str()) == Some("InternalIP")
            && address.get("address").and_then(|value| value.as_str()) == Some("172.31.11.2")
    }));
    assert!(
        addresses.iter().any(|address| {
            address.get("type").and_then(|value| value.as_str()) == Some("ExternalIP")
                && address.get("address").and_then(|value| value.as_str()) == Some("10.99.0.11")
        }),
        "NodeRegistration should publish dataplane endpoint as ExternalIP without using InternalIP: {addresses:?}",
    );
}

#[tokio::test]
async fn raft_node_subnet_replay_is_deterministic() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    let put = LogApplyNodeSubnetRow {
        node_name: "node-alpha".to_string(),
        subnet: "10.60.0.0/24".to_string(),
        subnet_base_int: u32::from(Ipv4Addr::new(10, 60, 0, 0)),
        vtep_ip: "10.60.0.1".to_string(),
        vtep_mac: Some("AA:BB:CC:DD:EE:01".to_string()),
        node_ip: "192.0.2.10".to_string(),
        mode: "root".to_string(),
        hostport_range: Some("30000-30100".to_string()),
    };
    let put_commit = LogApplyCommit::new(1, vec![LogApplyMutation::PutNodeSubnet(put.clone())]);
    leader
        .apply_log_apply_commit(put_commit.clone())
        .await
        .unwrap();
    follower.apply_log_apply_commit(put_commit).await.unwrap();
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        vec![(
            "node-alpha".to_string(),
            "10.60.0.0/24".to_string(),
            i64::from(u32::from(Ipv4Addr::new(10, 60, 0, 0))),
            "10.60.0.1".to_string(),
            Some("AA:BB:CC:DD:EE:01".to_string()),
            "192.0.2.10".to_string(),
            "root".to_string(),
            Some("30000-30100".to_string()),
            0,
        )],
        "leader/follower must store deterministic node_subnet state after put replay",
    );
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        select_node_subnet_rows(&follower).await,
        "node subnet rows must be deterministic between leader and follower after put replay",
    );

    let allocate_commit = LogApplyCommit::new(
        2,
        vec![LogApplyMutation::AllocateNodeSubnet(
            LogApplyNodeSubnetAllocation {
                node_name: "node-beta".to_string(),
                cluster_cidr: "10.80.0.0/16".to_string(),
                node_ip: "192.0.2.11".to_string(),
            },
        )],
    );
    leader
        .apply_log_apply_commit(allocate_commit.clone())
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(allocate_commit)
        .await
        .unwrap();
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        vec![
            (
                "node-alpha".to_string(),
                "10.60.0.0/24".to_string(),
                i64::from(u32::from(Ipv4Addr::new(10, 60, 0, 0))),
                "10.60.0.1".to_string(),
                Some("AA:BB:CC:DD:EE:01".to_string()),
                "192.0.2.10".to_string(),
                "root".to_string(),
                Some("30000-30100".to_string()),
                0,
            ),
            (
                "node-beta".to_string(),
                "10.80.0.0/24".to_string(),
                i64::from(u32::from(Ipv4Addr::new(10, 80, 0, 0))),
                "10.80.0.0".to_string(),
                None,
                "192.0.2.11".to_string(),
                "root".to_string(),
                None,
                0,
            ),
        ],
        "allocated and existing subnet rows must be deterministic",
    );
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        select_node_subnet_rows(&follower).await,
        "node subnet rows must match between leader and follower after allocation replay",
    );

    let delete_commit = LogApplyCommit::new(
        3,
        vec![LogApplyMutation::DeleteNodeSubnet {
            node_name: "node-alpha".to_string(),
        }],
    );
    leader
        .apply_log_apply_commit(delete_commit.clone())
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(delete_commit)
        .await
        .unwrap();
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        vec![(
            "node-beta".to_string(),
            "10.80.0.0/24".to_string(),
            i64::from(u32::from(Ipv4Addr::new(10, 80, 0, 0))),
            "10.80.0.0".to_string(),
            None,
            "192.0.2.11".to_string(),
            "root".to_string(),
            None,
            0,
        )],
        "deleted node must be absent after deterministic delete replay",
    );
    assert!(
        !select_node_subnet_rows(&leader)
            .await
            .iter()
            .any(|row| row.0 == "node-alpha"),
        "deleted node must be absent in deterministic replay",
    );
    assert_eq!(
        select_node_subnet_rows(&leader).await,
        select_node_subnet_rows(&follower).await,
        "node subnet replay must stay identical after delete",
    );
}

#[tokio::test]
async fn raft_node_dataplane_replay_is_deterministic() {
    let leader = Datastore::new_in_memory().await.unwrap();
    let follower = Datastore::new_in_memory().await.unwrap();

    let put = LogApplyNodeDataplaneRow {
        node_name: "node-gamma".to_string(),
        mode: "root".to_string(),
        encryption: "enabled".to_string(),
        public_key: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
        endpoint: "10.99.0.10".to_string(),
        port: Some(51820),
    };
    let put_commit = LogApplyCommit::new(1, vec![LogApplyMutation::PutNodeDataplane(put)]);
    leader
        .apply_log_apply_commit(put_commit.clone())
        .await
        .unwrap();
    follower.apply_log_apply_commit(put_commit).await.unwrap();
    assert_eq!(
        select_node_dataplane_rows(&leader).await,
        vec![(
            "node-gamma".to_string(),
            "root".to_string(),
            "enabled".to_string(),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string()),
            "10.99.0.10".to_string(),
            Some(51820),
            0
        )],
        "leader node_dataplane rows must be deterministic after put replay",
    );
    assert_eq!(
        select_node_dataplane_rows(&leader).await,
        select_node_dataplane_rows(&follower).await,
        "node_dataplane replay must be byte-identical between leader and follower",
    );

    let delete_commit = LogApplyCommit::new(
        2,
        vec![LogApplyMutation::DeleteNodeDataplane {
            node_name: "node-gamma".to_string(),
        }],
    );
    leader
        .apply_log_apply_commit(delete_commit.clone())
        .await
        .unwrap();
    follower
        .apply_log_apply_commit(delete_commit)
        .await
        .unwrap();
    assert_eq!(
        select_node_dataplane_rows(&leader).await,
        Vec::<NodeDataplaneReplayRow>::new(),
        "deleted node_dataplane row must be absent after delete replay",
    );
    assert_eq!(
        select_node_dataplane_rows(&leader).await,
        select_node_dataplane_rows(&follower).await,
        "node_dataplane rows must be deterministic between leader and follower after delete replay",
    );
}

#[tokio::test]
async fn test_db_create_resource_sets_uid() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({
        "metadata": {
            "name": "test-pod"
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();

    // Verify uid was set
    let uid = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("uid"))
        .and_then(|u| u.as_str());
    assert!(uid.is_some(), "uid should be set");
    assert!(!uid.unwrap().is_empty(), "uid should not be empty");
}

#[tokio::test]
async fn test_db_create_resource_sets_generation() {
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({
        "metadata": {
            "name": "test-deployment"
        },
        "spec": {}
    });
    let resource = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-deployment",
            data,
        )
        .await
        .unwrap();

    // Verify generation was set to 1
    let generation = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(|g| g.as_i64());
    assert_eq!(generation, Some(1), "generation should be set to 1");
}

#[tokio::test]
async fn test_db_create_resource_handles_generation_zero() {
    let db = Datastore::new_in_memory().await.unwrap();
    // kubectl sends generation: 0
    let data = json!({
        "metadata": {
            "name": "test-deployment",
            "generation": 0
        },
        "spec": {}
    });
    let resource = db
        .create_resource(
            "apps/v1",
            "Deployment",
            Some("default"),
            "test-deployment",
            data,
        )
        .await
        .unwrap();

    // Verify generation was set to 1 (not left at 0)
    let generation = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("generation"))
        .and_then(|g| g.as_i64());
    assert_eq!(
        generation,
        Some(1),
        "generation: 0 should be replaced with 1"
    );
}

#[tokio::test]
async fn test_sa_volume_injection_default_adds_projected_volume() {
    let db = Datastore::new_in_memory().await.unwrap();
    // Create pod without automountServiceAccountToken field (defaults to true)
    let data = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "containers": [{
                "name": "test",
                "image": "busybox"
            }]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();

    // Verify projected volume was injected
    let volumes = resource
        .data
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array());
    assert!(volumes.is_some(), "volumes should be injected");
    let volumes = volumes.unwrap();
    assert_eq!(volumes.len(), 1, "should have 1 volume");

    // Check volume structure
    let vol = &volumes[0];
    let vol_name = vol.get("name").and_then(|n| n.as_str());
    assert!(vol_name.is_some(), "volume should have name");
    assert!(
        vol_name.unwrap().starts_with("kube-api-access-"),
        "volume name should start with kube-api-access-"
    );

    // Check projected volume sources
    let sources = vol.pointer("/projected/sources").and_then(|s| s.as_array());
    assert!(sources.is_some(), "projected volume should have sources");
    let sources = sources.unwrap();
    assert_eq!(
        sources.len(),
        3,
        "should have 3 sources: serviceAccountToken, configMap, downwardAPI"
    );

    // Verify volumeMount was added to container
    let volume_mounts = resource
        .data
        .pointer("/spec/containers/0/volumeMounts")
        .and_then(|v| v.as_array());
    assert!(
        volume_mounts.is_some(),
        "volumeMounts should be added to container"
    );
    let mounts = volume_mounts.unwrap();
    assert_eq!(mounts.len(), 1, "should have 1 volumeMount");
    assert_eq!(
        mounts[0].get("mountPath").and_then(|p| p.as_str()),
        Some("/var/run/secrets/kubernetes.io/serviceaccount"),
        "mountPath should be /var/run/secrets/kubernetes.io/serviceaccount"
    );
    assert_eq!(
        mounts[0].get("readOnly").and_then(|r| r.as_bool()),
        Some(true),
        "volumeMount should be readOnly"
    );
}

#[tokio::test]
async fn test_sa_volume_injection_explicit_false_skips() {
    let db = Datastore::new_in_memory().await.unwrap();
    // Create pod with automountServiceAccountToken: false
    let data = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "automountServiceAccountToken": false,
            "containers": [{
                "name": "test",
                "image": "busybox"
            }]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();

    // Verify NO volume was injected
    let volumes = resource
        .data
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array());
    assert!(
        volumes.is_none() || volumes.unwrap().is_empty(),
        "no volume should be injected when automountServiceAccountToken is false"
    );

    // Verify NO volumeMount was added
    let volume_mounts = resource
        .data
        .pointer("/spec/containers/0/volumeMounts")
        .and_then(|v| v.as_array());
    assert!(
        volume_mounts.is_none() || volume_mounts.unwrap().is_empty(),
        "no volumeMount should be added when automountServiceAccountToken is false"
    );
}

#[tokio::test]
async fn test_sa_volume_injection_serviceaccount_false_skips() {
    let db = Datastore::new_in_memory().await.unwrap();

    let sa = json!({
        "metadata": {
            "name": "nomount",
            "namespace": "default"
        },
        "automountServiceAccountToken": false
    });
    db.create_resource("v1", "ServiceAccount", Some("default"), "nomount", sa)
        .await
        .unwrap();

    let pod = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "serviceAccountName": "nomount",
            "containers": [{
                "name": "test",
                "image": "busybox"
            }]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let volumes = resource
        .data
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array());
    assert!(
        volumes.is_none() || volumes.unwrap().is_empty(),
        "SA automount=false must skip pod SA volume injection"
    );
}

#[tokio::test]
async fn test_sa_volume_injection_pod_true_overrides_serviceaccount_false() {
    let db = Datastore::new_in_memory().await.unwrap();

    let sa = json!({
        "metadata": {
            "name": "nomount",
            "namespace": "default"
        },
        "automountServiceAccountToken": false
    });
    db.create_resource("v1", "ServiceAccount", Some("default"), "nomount", sa)
        .await
        .unwrap();

    let pod = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "serviceAccountName": "nomount",
            "automountServiceAccountToken": true,
            "containers": [{
                "name": "test",
                "image": "busybox"
            }]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", pod)
        .await
        .unwrap();

    let volumes = resource
        .data
        .get("spec")
        .and_then(|s| s.get("volumes"))
        .and_then(|v| v.as_array());
    assert!(
        volumes.is_some(),
        "pod-level automount=true must override SA=false"
    );
    assert!(
        !volumes.unwrap().is_empty(),
        "projected SA volume must be injected"
    );
}

#[tokio::test]
async fn test_sa_volume_mount_added_to_all_containers() {
    let db = Datastore::new_in_memory().await.unwrap();
    // Create pod with 2 containers and 1 init container
    let data = json!({
        "metadata": {
            "name": "test-pod",
            "namespace": "default"
        },
        "spec": {
            "initContainers": [{
                "name": "init",
                "image": "busybox"
            }],
            "containers": [
                {
                    "name": "app",
                    "image": "nginx"
                },
                {
                    "name": "sidecar",
                    "image": "busybox"
                }
            ]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "test-pod", data)
        .await
        .unwrap();

    // Verify volumeMount added to init container
    let init_mounts = resource
        .data
        .pointer("/spec/initContainers/0/volumeMounts")
        .and_then(|v| v.as_array());
    assert!(
        init_mounts.is_some(),
        "volumeMounts should be added to init container"
    );
    assert_eq!(
        init_mounts.unwrap().len(),
        1,
        "init container should have 1 volumeMount"
    );

    // Verify volumeMount added to both regular containers
    let app_mounts = resource
        .data
        .pointer("/spec/containers/0/volumeMounts")
        .and_then(|v| v.as_array());
    assert!(
        app_mounts.is_some(),
        "volumeMounts should be added to app container"
    );
    assert_eq!(
        app_mounts.unwrap().len(),
        1,
        "app container should have 1 volumeMount"
    );

    let sidecar_mounts = resource
        .data
        .pointer("/spec/containers/1/volumeMounts")
        .and_then(|v| v.as_array());
    assert!(
        sidecar_mounts.is_some(),
        "volumeMounts should be added to sidecar container"
    );
    assert_eq!(
        sidecar_mounts.unwrap().len(),
        1,
        "sidecar container should have 1 volumeMount"
    );
}

#[tokio::test]
async fn test_create_resource_sets_timestamp_even_when_null() {
    // Bug: clients (kubectl, sonobuoy) may send creationTimestamp: null.
    // contains_key("creationTimestamp") returns true for null values,
    // so the timestamp injection was skipped, leaving AGE as <unknown>.
    let db = Datastore::new_in_memory().await.unwrap();
    let data = json!({
        "metadata": {
            "name": "null-ts-pod",
            "namespace": "default",
            "creationTimestamp": null
        },
        "spec": {
            "containers": [{"name": "test", "image": "busybox"}]
        }
    });
    let resource = db
        .create_resource("v1", "Pod", Some("default"), "null-ts-pod", data)
        .await
        .unwrap();

    let ts = resource
        .data
        .pointer("/metadata/creationTimestamp")
        .and_then(|t| t.as_str());
    assert!(
        ts.is_some(),
        "creationTimestamp must be set even when client sends null, got: {:?}",
        resource.data.pointer("/metadata/creationTimestamp")
    );
    assert!(
        chrono::DateTime::parse_from_rfc3339(ts.unwrap()).is_ok(),
        "creationTimestamp must be valid RFC3339, got: {}",
        ts.unwrap()
    );
}

// ========================
// Pagination (chunking) tests
// ========================

#[tokio::test]
async fn test_list_resources_pagination_returns_items_sorted_by_name() {
    let db = Datastore::new_in_memory().await.unwrap();

    // Create 3 resources in non-alphabetical insertion order
    for name in ["charlie", "alpha", "bravo"] {
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            name,
            json!({"metadata": {"name": name, "namespace": "default"}}),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    let names: Vec<&str> = list.items.iter().map(|r| r.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["alpha", "bravo", "charlie"],
        "Items must be sorted alphabetically by name"
    );
}

#[tokio::test]
async fn test_list_resources_pagination_first_page_sets_continue_token() {
    let db = Datastore::new_in_memory().await.unwrap();

    for i in 0..5u32 {
        let name = format!("pod-{:04}", i);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name.clone(),
            json!({"metadata": {"name": name, "namespace": "default"}}),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(3), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3, "First page must have 3 items");
    assert!(
        list.continue_token.is_some(),
        "continue_token must be set when more items exist"
    );
    assert!(
        list.remaining_item_count.is_some(),
        "remainingItemCount must be set when more items exist"
    );
    // We fetch lim+1=4 rows; after truncating to 3, remaining = 4-3 = 1.
    // K8s treats remainingItemCount as an estimate (lower bound is fine).
    assert!(
        list.remaining_item_count.unwrap() >= 1,
        "remainingItemCount must be >= 1 when more pages exist"
    );

    // Continue token must be the last name on this page
    let last_name = list.items.last().unwrap().name.clone();
    assert_eq!(
        list.continue_token.as_ref().unwrap(),
        &last_name,
        "Continue token must be the last name on the page"
    );
}

#[tokio::test]
async fn test_list_resources_pagination_second_page_resumes_correctly() {
    let db = Datastore::new_in_memory().await.unwrap();

    for i in 0..5u32 {
        let name = format!("pod-{:04}", i);
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            &name.clone(),
            json!({"metadata": {"name": name, "namespace": "default"}}),
        )
        .await
        .unwrap();
    }

    // Get first page
    let page1 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(3), None),
        )
        .await
        .unwrap();

    let token = page1
        .continue_token
        .clone()
        .expect("First page must have continue token");

    // Get second page using continue token
    let page2 = db
        .list_resources(
            "v1",
            "Pod",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(3), Some(&token)),
        )
        .await
        .unwrap();

    assert_eq!(
        page2.items.len(),
        2,
        "Second page must have remaining 2 items"
    );
    assert!(
        page2.continue_token.is_none(),
        "No more pages — continue_token must be None"
    );
    assert!(
        page2.remaining_item_count.is_none(),
        "No more pages — remainingItemCount must be None"
    );

    // Verify all 5 names across both pages in order
    let all_names: Vec<&str> = page1
        .items
        .iter()
        .chain(page2.items.iter())
        .map(|r| r.name.as_str())
        .collect();
    assert_eq!(
        all_names,
        vec!["pod-0000", "pod-0001", "pod-0002", "pod-0003", "pod-0004"]
    );
}

#[tokio::test]
async fn test_list_resources_pagination_no_limit_has_no_continue_token() {
    let db = Datastore::new_in_memory().await.unwrap();

    for i in 0..3u32 {
        let name = format!("cm-{}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            json!({"metadata": {"name": name}}),
        )
        .await
        .unwrap();
    }

    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::all(),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(
        list.continue_token.is_none(),
        "No limit — continue_token must be None"
    );
    assert!(
        list.remaining_item_count.is_none(),
        "No limit — remainingItemCount must be None"
    );
}

#[tokio::test]
async fn test_list_resources_pagination_exact_page_size_has_no_continue_token() {
    let db = Datastore::new_in_memory().await.unwrap();

    for i in 0..3u32 {
        let name = format!("cm-{}", i);
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("default"),
            &name.clone(),
            json!({"metadata": {"name": name}}),
        )
        .await
        .unwrap();
    }

    // limit equals total — no next page
    let list = db
        .list_resources(
            "v1",
            "ConfigMap",
            Some("default"),
            crate::datastore::ResourceListQuery::new(None, None, Some(3), None),
        )
        .await
        .unwrap();

    assert_eq!(list.items.len(), 3);
    assert!(
        list.continue_token.is_none(),
        "Exact fit — continue_token must be None"
    );
    assert!(
        list.remaining_item_count.is_none(),
        "Exact fit — remainingItemCount must be None"
    );
}

/// Verify that after create_resource, the stored data blob includes metadata.labels.
/// This is a prerequisite for the broadcast path: update_hook reads back the same
/// data blob and assembles the WatchEvent — if labels are stored, they will be in
/// the broadcast event.
#[tokio::test]
async fn test_stored_data_includes_labels_after_create() {
    let db = Datastore::new_in_memory().await.unwrap();

    let data = json!({
        "metadata": {
            "name": "cm-with-labels",
            "labels": {
                "watch-this-configmap": "multiple-watchers-A",
                "env": "test"
            }
        },
        "data": {
            "key": "value"
        }
    });

    db.create_resource("v1", "ConfigMap", Some("default"), "cm-with-labels", data)
        .await
        .unwrap();

    // Read back via get_resource — same query the update_hook uses
    let resource = db
        .get_resource("v1", "ConfigMap", Some("default"), "cm-with-labels")
        .await
        .unwrap()
        .expect("ConfigMap must exist after create");

    // Labels must be present at metadata.labels in the stored data blob
    let labels = resource
        .data
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .expect("metadata.labels must be stored in DB data blob");

    assert_eq!(
        labels["watch-this-configmap"], "multiple-watchers-A",
        "Label 'watch-this-configmap' must be stored in DB"
    );
    assert_eq!(labels["env"], "test", "Label 'env' must be stored in DB");
}

/// Verify that hydrate_watch_event_data (injecting kind/namespace/name/rv into
/// the data blob) preserves metadata.labels.
#[test]
fn test_update_hook_data_assembly_preserves_labels() {
    use crate::watch::WatchEvent;

    // Simulate the data blob as it comes out of the DB (without injected fields)
    let data = json!({
        "metadata": {
            "name": "cm-with-labels",
            "labels": {
                "watch-this-configmap": "multiple-watchers-A",
                "env": "test"
            }
        },
        "data": {"key": "value"}
    });

    let data = hydrate_watch_event_data(
        data,
        "v1",
        "ConfigMap",
        Some("default"),
        "cm-with-labels",
        42,
    );

    let event = WatchEvent::added(data);

    // Labels must survive the metadata injection
    let labels = event
        .object
        .get("metadata")
        .and_then(|m| m.get("labels"))
        .expect("metadata.labels must be preserved after update_hook data assembly");

    assert_eq!(labels["watch-this-configmap"], "multiple-watchers-A");
    assert_eq!(labels["env"], "test");

    // And the injected fields must also be present
    assert_eq!(event.object["kind"], "ConfigMap");
    assert_eq!(event.object["metadata"]["name"], "cm-with-labels");
    assert_eq!(event.object["metadata"]["namespace"], "default");
    assert_eq!(event.object["metadata"]["resourceVersion"], "42");

    // Verify matches_filter uses the labels correctly
    assert!(
        event.matches_filter(
            "ConfigMap",
            Some("default"),
            Some("watch-this-configmap=multiple-watchers-A")
        ),
        "Event with label=A must match selector=A"
    );
    assert!(
        !event.matches_filter(
            "ConfigMap",
            Some("default"),
            Some("watch-this-configmap=multiple-watchers-B")
        ),
        "Event with label=A must NOT match selector=B (equality filter)"
    );
    assert!(
        event.matches_filter(
            "ConfigMap",
            Some("default"),
            Some("watch-this-configmap!=multiple-watchers-B")
        ),
        "Event with label=A must match selector !=B (inequality filter)"
    );
    assert!(
        !event.matches_filter(
            "ConfigMap",
            Some("default"),
            Some("watch-this-configmap!=multiple-watchers-A")
        ),
        "Event with label=A must NOT match selector !=A (inequality filter — same value)"
    );
}

#[test]
fn test_update_hook_data_assembly_preserves_empty_namespace_for_cluster_scoped_object() {
    use crate::watch::WatchEvent;

    let data = json!({
        "metadata": {
            "name": "cluster-cr",
            "namespace": ""
        },
        "content": {"key": "value"}
    });

    let data = hydrate_watch_event_data(
        data,
        "mygroup.example.com/v1beta1",
        "WishIHadChosenNoxu",
        None,
        "cluster-cr",
        99,
    );
    let event = WatchEvent::added(data);

    assert_eq!(
        event.object["metadata"]["namespace"], "",
        "cluster-scoped watch event assembly must preserve existing empty namespace"
    );
    assert_eq!(event.object["metadata"]["resourceVersion"], "99");
}

// ========================
// Field selector filtering tests
// ========================
