pub mod apply;
pub mod informer;
pub mod leader_proxy;
pub mod local;
pub mod membership;
pub mod pod_status_side_effects;
pub mod remote;
pub mod worker_store;

use std::pin::Pin;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

use crate::datastore::{NodeSubnet, PodCleanupIntent, Resource, ResourceList};
use crate::kubelet::outbox::payload::OutboxOperation;
use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
use crate::networking::wireguard::DataplanePeerMetadata;
use crate::watch::WatchEvent;

pub type Pod = Resource;
pub type ConfigMap = Resource;
pub type Secret = Resource;
pub type Node = Resource;
pub type WatchStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceKey {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub limit: Option<i64>,
    pub continue_token: Option<String>,
}

pub type ListResponse = ResourceList;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchRequest {
    pub api_version: String,
    pub kind: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
    pub start_resource_version: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedServiceAccountTokenRequest {
    pub namespace: String,
    pub service_account_name: String,
    pub audiences: Vec<String>,
    pub expiration_seconds: i64,
    pub bound_pod_name: Option<String>,
    pub bound_pod_uid: Option<String>,
    pub bound_node_name: Option<String>,
    pub bound_node_uid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedServiceAccountToken {
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CacheScope {
    Cluster,
    Resource {
        api_version: String,
        kind: String,
        namespace: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct ResourceEvent {
    pub event: WatchEvent,
}

#[async_trait]
pub trait LeaderApiClient: Send + Sync {
    async fn get_resource(&self, key: ResourceKey) -> Result<Option<Resource>>;
    async fn list_resources(&self, req: ListRequest) -> Result<ListResponse>;
    async fn get_resource_fresh(&self, key: ResourceKey) -> Result<Option<Resource>> {
        self.get_resource(key).await
    }
    async fn list_resources_fresh(&self, req: ListRequest) -> Result<ListResponse> {
        self.list_resources(req).await
    }
    async fn watch_resources(&self, req: WatchRequest) -> Result<WatchStream<ResourceEvent>>;
    async fn wait_cache_ready(&self, scope: CacheScope) -> Result<()>;
    async fn projected_service_account_token(
        &self,
        request: ProjectedServiceAccountTokenRequest,
    ) -> Result<ProjectedServiceAccountToken> {
        let _ = request;
        anyhow::bail!("projected ServiceAccount token requests are not supported by this client")
    }

    async fn get_pod(&self, ns: &str, name: &str) -> Result<Option<Pod>>;
    async fn get_pod_for_uid(&self, ns: &str, name: &str, uid: &str) -> Result<Option<Pod>>;
    async fn watch_pods_on_node(&self, node_name: &str) -> Result<WatchStream<Pod>>;
    async fn list_pods_on_node(&self, node_name: &str) -> Result<Vec<Pod>>;
    async fn get_configmap(&self, ns: &str, name: &str) -> Result<Option<ConfigMap>>;
    async fn get_secret(&self, ns: &str, name: &str) -> Result<Option<Secret>>;
    async fn get_node(&self, name: &str) -> Result<Node>;
    async fn watch_node(&self, name: &str) -> Result<WatchStream<Node>>;
    async fn allocate_node_subnet(
        &self,
        node_name: &str,
        cluster_cidr: &str,
        node_ip: &str,
    ) -> Result<NodeSubnet>;
    async fn get_node_subnet(&self, node_name: &str) -> Result<Option<NodeSubnet>>;
    async fn list_peer_subnets(&self, my_node_name: &str) -> Result<Vec<NodeSubnet>>;
    async fn get_node_dataplane(&self, node_name: &str) -> Result<Option<DataplanePeerMetadata>>;
    async fn list_pod_cleanup_intents_for_node(
        &self,
        node_name: &str,
    ) -> Result<Vec<PodCleanupIntent>> {
        let _ = node_name;
        Ok(Vec::new())
    }
    async fn delete_pod_cleanup_intent(
        &self,
        node_name: &str,
        namespace: &str,
        pod_name: &str,
        pod_uid: &str,
        reason: &str,
    ) -> Result<()> {
        let _ = (node_name, namespace, pod_name, pod_uid, reason);
        Ok(())
    }
    async fn get_cluster_membership(&self) -> Result<membership::ClusterMembership> {
        anyhow::bail!("cluster membership RPC is not implemented by this client")
    }

    async fn apply_outbox(
        &self,
        idempotency_key: &str,
        operation: OutboxOperation,
        payload: Bytes,
    ) -> std::result::Result<OutboxApplyResult, OutboxApplyError>;
}

#[cfg(test)]
mod tests {
    mod t10_tests;

    use std::sync::Arc;

    use bytes::Bytes;

    use crate::control_plane::client::LeaderApiClient;
    use crate::control_plane::client::local::LocalApiClient;
    use crate::control_plane::client::membership;
    use crate::datastore::ResourcePreconditions;
    use crate::datastore::backend::DatastoreBackend;
    use crate::datastore::command::StorageCommand;
    use crate::kubelet::outbox::payload::{OutboxOperation, OutboxPayload};
    use crate::kubelet::outbox::{OutboxApplyError, OutboxApplyResult};
    use crate::networking::wireguard::{DataplaneEncryption, DataplaneMode, DataplanePeerMetadata};

    fn pod_status_payload(uid: &str) -> Bytes {
        let command = StorageCommand::UpdateStatus {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some("default".to_string()),
            name: "web".to_string(),
            status: serde_json::json!({"phase": "Running"}),
            expected_rv: None,
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
            observed_status_stamp: None,
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode payload"),
        )
    }

    fn pod_delete_payload(name: &str, uid: &str) -> Bytes {
        pod_delete_payload_for("default", name, uid)
    }

    fn pod_delete_payload_for(namespace: &str, name: &str, uid: &str) -> Bytes {
        let command = StorageCommand::DeleteResource {
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(namespace.to_string()),
            name: name.to_string(),
            preconditions: ResourcePreconditions {
                uid: Some(uid.to_string()),
                resource_version: None,
            },
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode payload"),
        )
    }

    fn node_lease_renew_payload(node_name: &str, renew_time: &str) -> Bytes {
        let command = StorageCommand::CreateResource {
            api_version: "coordination.k8s.io/v1".to_string(),
            kind: "Lease".to_string(),
            namespace: Some("kube-node-lease".to_string()),
            name: node_name.to_string(),
            data: serde_json::json!({
                "apiVersion": "coordination.k8s.io/v1",
                "kind": "Lease",
                "metadata": {
                    "name": node_name,
                    "namespace": "kube-node-lease"
                },
                "spec": {
                    "holderIdentity": node_name,
                    "leaseDurationSeconds": 50,
                    "renewTime": renew_time
                }
            }),
        };
        Bytes::from(
            OutboxPayload::from_command(command)
                .encode_protobuf()
                .expect("encode lease renew payload"),
        )
    }

    #[tokio::test]
    async fn local_client_reads_pods_and_filters_uid() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx"}]}
            }),
        )
        .await
        .expect("create pod");
        let client = LocalApiClient::new(
            Arc::new(db),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        assert!(
            client
                .get_pod("default", "web")
                .await
                .expect("get pod")
                .is_some()
        );
        assert!(
            client
                .get_pod_for_uid("default", "web", "uid-1")
                .await
                .expect("get pod for uid")
                .is_some()
        );
        assert!(
            client
                .get_pod_for_uid("default", "web", "uid-2")
                .await
                .expect("get stale uid")
                .is_none()
        );
    }

    #[tokio::test]
    async fn local_client_apply_outbox_is_idempotent_and_uid_bound() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create pod");
        let client = LocalApiClient::new(
            Arc::new(db.clone()),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        let first = LeaderApiClient::apply_outbox(
            &client,
            "stable-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
        )
        .await
        .expect("first apply");
        let duplicate = LeaderApiClient::apply_outbox(
            &client,
            "stable-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
        )
        .await
        .expect("duplicate apply");
        assert!(matches!(first, OutboxApplyResult::Applied { .. }));
        assert!(matches!(
            duplicate,
            OutboxApplyResult::AlreadyApplied { .. }
        ));
        let stored = db
            .get_resource("v1", "Pod", Some("default"), "web")
            .await
            .expect("get pod")
            .expect("pod exists");
        assert_eq!(
            stored
                .data
                .pointer("/status/phase")
                .and_then(|v| v.as_str()),
            Some("Running")
        );

        let err = LeaderApiClient::apply_outbox(
            &client,
            "uid-mismatch-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-2"),
        )
        .await
        .expect_err("uid mismatch");
        assert!(matches!(err, OutboxApplyError::UidMismatch { .. }));
    }

    #[tokio::test]
    async fn local_client_apply_outbox_advances_n1_raft_commit_index() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "web",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "default",
                    "name": "web",
                    "uid": "uid-1"
                },
                "spec": {"containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Pending"}
            }),
        )
        .await
        .expect("create pod");
        let client = LocalApiClient::new(
            Arc::new(db),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        assert_eq!(client.last_raft_commit_index_for_test().await, 0);
        let applied = LeaderApiClient::apply_outbox(
            &client,
            "raft-client-key",
            OutboxOperation::PodStatus,
            pod_status_payload("uid-1"),
        )
        .await
        .expect("apply outbox through local client");

        let OutboxApplyResult::Applied { applied_rv } = applied else {
            panic!("first local apply must commit a new write");
        };
        assert_eq!(client.last_raft_commit_index_for_test().await, applied_rv);
    }

    #[tokio::test]
    async fn local_client_lease_renew_updates_memory_without_cluster_db_write() {
        let db = crate::datastore::test_support::in_memory().await;
        let tracker = Arc::new(crate::node_lease_tracker::NodeLeaseTracker::new_for_test(
            chrono::DateTime::parse_from_rfc3339("2026-05-25T14:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        ));
        let client = LocalApiClient::new_with_node_lease_tracker(
            Arc::new(db.clone()),
            "worker-a".to_string(),
            tracker.clone(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let before_rv = db.get_current_resource_version().await.unwrap();

        let applied = LeaderApiClient::apply_outbox(
            &client,
            "lease-renew-memory-key",
            OutboxOperation::LeaseRenew,
            node_lease_renew_payload("worker-a", "2026-05-25T14:00:10.000000Z"),
        )
        .await
        .expect("lease renew should be accepted");

        assert!(matches!(
            applied,
            OutboxApplyResult::Applied { applied_rv: 0 }
        ));
        assert_eq!(
            db.get_current_resource_version().await.unwrap(),
            before_rv,
            "node Lease renew must not advance cluster resourceVersion"
        );
        assert!(
            db.get_resource(
                "coordination.k8s.io/v1",
                "Lease",
                Some("kube-node-lease"),
                "worker-a",
            )
            .await
            .unwrap()
            .is_none(),
            "node Lease renew must not create or update a cluster.db Lease row"
        );
        assert!(
            db.list_applied_outbox().await.unwrap().is_empty(),
            "in-memory heartbeats must not write the replicated applied_outbox ledger"
        );
        // T3: `list_log_apply_entries_after` removed.
        let observed = tracker
            .observed("worker-a")
            .await
            .expect("heartbeat tracked");
        assert_eq!(observed.renew_time_string(), "2026-05-25T14:00:10Z");
    }

    #[tokio::test]
    async fn local_client_returns_cluster_membership() {
        let db = crate::datastore::test_support::in_memory().await;
        crate::bootstrap::cluster_meta::ensure_cluster_metadata(&db)
            .await
            .expect("ensure metadata");
        let cluster_id = db
            .get_klights_meta(crate::bootstrap::cluster_meta::KEY_CLUSTER_ID)
            .await
            .unwrap()
            .unwrap();
        crate::bootstrap::cluster_meta::write_cluster_membership(
            &db,
            &membership::ClusterMembership {
                cluster_id: cluster_id.clone(),
                voters: vec!["mn-leader".to_string()],
                term: 1,
                leader_hint: Some("mn-leader".to_string()),
            },
        )
        .await
        .unwrap();

        let client = LocalApiClient::new(
            Arc::new(db),
            "mn-leader".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let membership = client.get_cluster_membership().await.unwrap();

        assert_eq!(membership.cluster_id, cluster_id);
        assert_eq!(membership.voters, vec!["mn-leader"]);
        assert_eq!(membership.term, 1);
        assert_eq!(membership.leader_hint.as_deref(), Some("mn-leader"));
    }

    #[tokio::test]
    async fn local_client_pod_delete_outbox_reconciles_terminating_namespace() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_namespace(
            "worker-finalize-ns",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": "worker-finalize-ns",
                    "uid": "worker-finalize-ns-uid"
                },
                "spec": {"finalizers": ["kubernetes"]},
                "status": {"phase": "Active"}
            }),
        )
        .await
        .expect("create namespace");
        let namespace = db
            .get_namespace("worker-finalize-ns")
            .await
            .expect("read namespace")
            .expect("namespace exists");
        let mut terminating = std::sync::Arc::unwrap_or_clone(namespace.data);
        crate::api::set_namespace_terminating_status(&mut terminating, false);
        db.update_namespace(
            "worker-finalize-ns",
            terminating,
            namespace.resource_version,
        )
        .await
        .expect("mark namespace terminating");
        db.create_resource(
            "v1",
            "ConfigMap",
            Some("worker-finalize-ns"),
            "leftover-cm",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {
                    "namespace": "worker-finalize-ns",
                    "name": "leftover-cm"
                },
                "data": {"k": "v"}
            }),
        )
        .await
        .expect("create non-pod content");
        db.create_resource(
            "v1",
            "Pod",
            Some("worker-finalize-ns"),
            "worker-pod",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "worker-finalize-ns",
                    "name": "worker-pod",
                    "uid": "worker-pod-uid",
                    "deletionTimestamp": "2026-05-20T00:00:00Z",
                    "deletionGracePeriodSeconds": 0
                },
                "spec": {
                    "nodeName": "worker-a",
                    "containers": [{"name": "app", "image": "nginx"}]
                },
                "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create terminating pod");

        let client = LocalApiClient::new(
            Arc::new(db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let applied = LeaderApiClient::apply_outbox(
            &client,
            "worker-pod-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            pod_delete_payload_for("worker-finalize-ns", "worker-pod", "worker-pod-uid"),
        )
        .await
        .expect("apply worker pod delete outbox");
        assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

        assert!(
            db.get_resource("v1", "Pod", Some("worker-finalize-ns"), "worker-pod")
                .await
                .expect("get pod")
                .is_none(),
            "leader apply must remove the actor-finalized Pod row"
        );
        assert!(
            db.get_namespace("worker-finalize-ns")
                .await
                .expect("get namespace")
                .is_none(),
            "leader must reconcile namespace termination immediately after applying worker Pod delete"
        );
    }

    #[tokio::test]
    async fn local_client_pod_delete_outbox_finalizes_ready_foreground_owner() {
        let db = crate::datastore::test_support::in_memory().await;
        db.create_resource(
            "v1",
            "ReplicationController",
            Some("default"),
            "foreground-owner",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ReplicationController",
                "metadata": {
                    "name": "foreground-owner",
                    "namespace": "default",
                    "uid": "foreground-owner-uid",
                    "deletionTimestamp": "2026-05-17T00:00:00Z",
                    "finalizers": ["foregroundDeletion"]
                },
                "spec": {"replicas": 1, "selector": {"app": "foreground-owner"}}
            }),
        )
        .await
        .expect("create foreground owner");
        db.create_resource(
            "v1",
            "Pod",
            Some("default"),
            "foreground-child",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "foreground-child",
                    "namespace": "default",
                    "uid": "foreground-child-uid",
                    "deletionTimestamp": "2026-05-17T00:00:00Z",
                    "deletionGracePeriodSeconds": 0,
                    "ownerReferences": [{
                        "apiVersion": "v1",
                        "kind": "ReplicationController",
                        "name": "foreground-owner",
                        "uid": "foreground-owner-uid",
                        "controller": true,
                        "blockOwnerDeletion": true
                    }]
                },
                "spec": {"nodeName": "worker-a", "containers": [{"name": "app", "image": "nginx"}]},
                "status": {"phase": "Running"}
            }),
        )
        .await
        .expect("create foreground child");

        let client = LocalApiClient::new(
            Arc::new(db.clone()),
            "worker-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );
        let dispatcher = Arc::new(crate::controller_dispatcher::ControllerDispatcher::new(
            Arc::new(crate::controllers::service::ServiceIpam::new(
                "10.43.128.0/17",
            )),
        ));
        dispatcher
            .set_pod_repository(crate::controllers::test_utils::pod_repository_for_test(&db))
            .await;
        client.set_controller_dispatcher(dispatcher);

        let applied = LeaderApiClient::apply_outbox(
            &client,
            "foreground-child-actor-finalize-delete",
            OutboxOperation::PodMetadata,
            pod_delete_payload("foreground-child", "foreground-child-uid"),
        )
        .await
        .expect("apply pod delete outbox");
        assert!(matches!(applied, OutboxApplyResult::Applied { .. }));

        assert!(
            db.get_resource("v1", "Pod", Some("default"), "foreground-child")
                .await
                .expect("get child")
                .is_none(),
            "leader apply must remove the finalized Pod row"
        );
        assert!(
            db.get_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "foreground-owner"
            )
            .await
            .expect("get foreground owner")
            .is_none(),
            "leader apply of the final dependent Pod delete must remove a ready foreground owner"
        );
    }

    #[tokio::test]
    async fn local_client_serves_network_metadata_without_calling_forwarder() {
        let db = crate::datastore::test_support::in_memory().await;
        let client = LocalApiClient::new(
            Arc::new(db.clone()),
            "node-a".to_string(),
            crate::control_plane::client::local::always_leader_watch(),
        );

        let subnet = client
            .allocate_node_subnet("node-a", "10.42.0.0/16", "192.0.2.10")
            .await
            .expect("allocate local subnet through leader API");
        assert_eq!(subnet.node_name.as_str(), "node-a");
        assert_eq!(subnet.subnet.to_string(), "10.42.0.0/24");

        let stored = client
            .get_node_subnet("node-a")
            .await
            .expect("get local subnet through leader API")
            .expect("allocated subnet should exist");
        assert_eq!(stored, subnet);

        let peer = client
            .allocate_node_subnet("node-b", "10.42.0.0/16", "192.0.2.11")
            .await
            .expect("allocate peer subnet");
        let peers = client
            .list_peer_subnets("node-a")
            .await
            .expect("list peer subnets through leader API");
        assert_eq!(peers, vec![peer]);

        let metadata = DataplanePeerMetadata::try_new(
            "node-b".to_string(),
            DataplaneMode::Root,
            DataplaneEncryption::Disabled,
            None,
            Some("192.0.2.11".to_string()),
            None,
        )
        .expect("valid dataplane metadata");
        db.update_node_dataplane(metadata.clone())
            .await
            .expect("store dataplane metadata");
        assert_eq!(
            client
                .get_node_dataplane("node-b")
                .await
                .expect("get dataplane metadata through leader API"),
            Some(metadata)
        );
    }
}
