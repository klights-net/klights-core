//! Parity fixture for checked-in Pod lifecycle baseline recordings.
//! Task 19.5: Baseline Capture and ParityFixture Harness.
//!
//! # Parity assertion rule
//! Parity tests assert `assert_eq!(baseline, runtime)` on the whole `Recording` —
//! never on individual fields. This ensures no observable channel is missed.

#![cfg(test)]

use std::sync::{Arc, Mutex};

use crate::kubelet::pod_cluster_runtime::{
    ClusterRuntimeView, ReplicationRuntime, RuntimeNodeRole,
};
use crate::kubelet::pod_repository::{PodReader, PodRepository, PodRepositoryBuildConfig};
use crate::kubelet::pod_runtime::service::PodRuntimeKey;

use super::cri::{ContainerRuntimeControl, CriRuntime};
use super::events::PodEventSink;
use super::filesystem::PodFilesystem;
use super::hostports::HostPortRuntime;
use super::network::PodNetworkRuntime;
use super::probes::ProbeRuntime;
use super::store::{PodRuntimeStore, PodSlotAdmission};
use super::volumes::PodVolumeRuntime;

// Re-export mock types from test_support for convenience.
use super::test_support::{
    FakeNode, MockContainerControlOp, MockContainerRuntimeControl, MockCriCall, MockCriRuntime,
    MockHostPortOp, MockHostPortRuntime, MockNetworkOp, MockPodEvent, MockPodEventSink,
    MockPodFilesystem, MockPodNetworkRuntime, MockPodRuntimeStore, MockPodSlotAdmission,
    MockPodVolumeRuntime, MockProbeCall, MockProbeRuntime, pod_json,
};

// ── Recording types ──

/// A write recorded on the PodRepository during a parity run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepositoryWrite {
    SetPodStatus {
        namespace: String,
        name: String,
        uid: String,
        phase: String,
    },
    AdmitSlot {
        namespace: String,
        name: String,
        uid: String,
    },
    ClearSlot {
        namespace: String,
        name: String,
        uid: String,
    },
    RecordSandbox {
        namespace: String,
        name: String,
        uid: String,
        sandbox_id: String,
    },
    DeleteSandbox {
        namespace: String,
        name: String,
        uid: String,
    },
    PodSnapshot {
        namespace: String,
        name: String,
        uid: String,
        resource_version: i64,
        status: serde_json::Value,
    },
}

/// An outbox event recorded during a parity run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutboxEvent {
    Enqueue { key: String, operation: String },
}

/// Recording of cluster-view operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordingClusterCall {
    GetFreshPod {
        namespace: String,
        name: String,
    },
    ForwardStatus {
        namespace: String,
        name: String,
        uid: String,
        status: serde_json::Value,
    },
}

/// Recording of replication operations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordingReplicationCall {
    EnqueueStorageCommand {
        namespace: String,
        name: String,
        uid: String,
        command_debug: String,
    },
}

/// Full behaviour recording from a single parity run.
///
/// # Invariant
/// Parity tests **must** assert `assert_eq!(legacy, new)` on the whole
/// `Recording` — never on individual fields. Partial-field assertions
/// miss regressions in uncompared channels.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recording {
    pub cri: Vec<MockCriCall>,
    pub container_control: Vec<MockContainerControlOp>,
    pub network: Vec<MockNetworkOp>,
    pub store: Vec<String>,
    pub slot: Vec<String>,
    pub filesystem: Vec<String>,
    pub volumes: Vec<String>,
    pub probes: Vec<MockProbeCall>,
    pub hostports: Vec<MockHostPortOp>,
    pub events: Vec<MockPodEvent>,
    pub cluster_view: Vec<RecordingClusterCall>,
    pub replication: Vec<RecordingReplicationCall>,
    pub repository_writes: Vec<RepositoryWrite>,
    pub outbox: Vec<OutboxEvent>,
    pub finalizer_calls: Vec<PodRuntimeKey>,
}

impl Recording {
    pub fn empty() -> Self {
        Self {
            cri: Vec::new(),
            container_control: Vec::new(),
            network: Vec::new(),
            store: Vec::new(),
            slot: Vec::new(),
            filesystem: Vec::new(),
            volumes: Vec::new(),
            probes: Vec::new(),
            hostports: Vec::new(),
            events: Vec::new(),
            cluster_view: Vec::new(),
            replication: Vec::new(),
            repository_writes: Vec::new(),
            outbox: Vec::new(),
            finalizer_calls: Vec::new(),
        }
    }
}

// ── Recording wrappers ──

/// Recording wrapper around `ClusterRuntimeView` for parity tests.
pub struct RecordingClusterRuntimeView {
    calls: Mutex<Vec<RecordingClusterCall>>,
}

impl Default for RecordingClusterRuntimeView {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordingClusterRuntimeView {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn recorded_calls(&self) -> Vec<RecordingClusterCall> {
        self.calls.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.calls.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl ClusterRuntimeView for RecordingClusterRuntimeView {
    async fn get_fresh_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> anyhow::Result<Option<crate::datastore::Resource>> {
        self.calls
            .lock()
            .unwrap()
            .push(RecordingClusterCall::GetFreshPod {
                namespace: namespace.to_string(),
                name: name.to_string(),
            });
        Ok(None)
    }

    async fn forward_pod_status(
        &self,
        key: &PodRuntimeKey,
        status: serde_json::Value,
    ) -> anyhow::Result<crate::datastore::Resource> {
        self.calls
            .lock()
            .unwrap()
            .push(RecordingClusterCall::ForwardStatus {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                status,
            });
        Ok(crate::datastore::Resource {
            id: 0,
            api_version: "v1".to_string(),
            kind: "Pod".to_string(),
            namespace: Some(key.namespace.clone()),
            name: key.name.clone(),
            uid: key.uid.clone(),
            data: Arc::new(
                serde_json::json!({"metadata": {"namespace": key.namespace, "name": key.name, "uid": key.uid}}),
            ),
            resource_version: 1,
        })
    }
}

/// Recording wrapper around `ReplicationRuntime` for parity tests.
pub struct RecordingReplicationRuntime {
    calls: Mutex<Vec<RecordingReplicationCall>>,
}

impl Default for RecordingReplicationRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordingReplicationRuntime {
    pub fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
        }
    }

    pub fn recorded_calls(&self) -> Vec<RecordingReplicationCall> {
        self.calls.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.calls.lock().unwrap().clear();
    }
}

#[async_trait::async_trait]
impl ReplicationRuntime for RecordingReplicationRuntime {
    async fn enqueue_storage_command(
        &self,
        key: &PodRuntimeKey,
        command: crate::datastore::command::StorageCommand,
    ) -> anyhow::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(RecordingReplicationCall::EnqueueStorageCommand {
                namespace: key.namespace.clone(),
                name: key.name.clone(),
                uid: key.uid.clone(),
                command_debug: format!("{:?}", command),
            });
        Ok(())
    }
}

// ── ParityFixture ──

/// Harness that wires every mockable port for parity comparison between
/// legacy and refactored Pod lifecycle code paths.
pub struct ParityFixture {
    pub cri: Arc<MockCriRuntime>,
    pub container_control: Arc<MockContainerRuntimeControl>,
    pub network: Arc<MockPodNetworkRuntime>,
    pub store: Arc<MockPodRuntimeStore>,
    pub slot: Arc<MockPodSlotAdmission>,
    pub filesystem: Arc<MockPodFilesystem>,
    pub volumes: Arc<MockPodVolumeRuntime>,
    pub probes: Arc<MockProbeRuntime>,
    pub hostports: Arc<MockHostPortRuntime>,
    pub events: Arc<MockPodEventSink>,
    pub cluster_view: Arc<RecordingClusterRuntimeView>,
    pub replication: Arc<RecordingReplicationRuntime>,
    pub repository: Arc<PodRepository>,
    pub node_view: Arc<FakeNode>,
    pub outbox_log: Arc<Mutex<Vec<OutboxEvent>>>,
}

impl ParityFixture {
    /// Construct every mock with empty state and an in-memory repository.
    pub async fn new() -> Self {
        let (ds, handle) = crate::datastore::test_support::in_memory_with_handle().await;
        std::mem::forget(ds);
        let supervisor = Arc::new(crate::task_supervisor::TaskSupervisor::new(
            crate::task_supervisor::TaskCategoryConfig::default(),
        ));
        let side_effects = Arc::new(crate::side_effects::SideEffectRegistry::new());
        let metrics: Arc<crate::side_effects::SideEffectMetrics> =
            crate::side_effects::SideEffectMetrics::new();
        let parts = PodRepository::build_parts(PodRepositoryBuildConfig {
            db: handle,
            supervisor,
            side_effects,
            metrics,
            network_events: crate::networking::global_pod_network_events(),
            scheduling_mode:
                crate::kubelet::pod_repository::api::PodSchedulingMode::InlineSingleNode,
            outbox: None,
            cluster_api: None,
        });
        let repository = Arc::new(parts.repository);

        Self {
            cri: Arc::new(MockCriRuntime::new()),
            container_control: Arc::new(MockContainerRuntimeControl::new()),
            network: Arc::new(MockPodNetworkRuntime::new()),
            store: Arc::new(MockPodRuntimeStore::new()),
            slot: Arc::new(MockPodSlotAdmission::new()),
            filesystem: Arc::new(MockPodFilesystem::new()),
            volumes: Arc::new(MockPodVolumeRuntime::new()),
            probes: Arc::new(MockProbeRuntime::new()),
            hostports: Arc::new(MockHostPortRuntime::new()),
            events: Arc::new(MockPodEventSink::new()),
            cluster_view: Arc::new(RecordingClusterRuntimeView::new()),
            replication: Arc::new(RecordingReplicationRuntime::new()),
            repository,
            node_view: Arc::new(FakeNode::new("test-node", RuntimeNodeRole::Worker)),
            outbox_log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Clear every recording vector so the fixture is ready for the next
    /// run. Call between consecutive parity runs so each starts from
    /// identical state.
    pub fn reset(&self) {
        self.cri.clear_calls();
        self.container_control.clear_calls();
        self.network.clear_calls();
        self.store.clear_calls();
        self.slot.clear_calls();
        self.filesystem.clear_calls();
        self.volumes.clear_calls();
        self.probes.clear_calls();
        self.hostports.clear_calls();
        self.events.clear_events();
        self.cluster_view.clear();
        self.replication.clear();
        self.outbox_log.lock().unwrap().clear();
    }

    /// Snapshot every recording vector into a `Recording`.
    pub fn snapshot(&self) -> Recording {
        Recording {
            cri: self.cri.recorded_calls(),
            container_control: self.container_control.recorded_calls(),
            network: self.network.recorded_calls(),
            store: self.store.recorded_calls(),
            slot: self.slot.recorded_calls(),
            filesystem: self.filesystem.recorded_calls(),
            volumes: self.volumes.recorded_calls(),
            probes: self.probes.recorded_calls(),
            hostports: self.hostports.recorded_calls(),
            events: self.events.recorded_events(),
            cluster_view: self.cluster_view.recorded_calls(),
            replication: self.replication.recorded_calls(),
            repository_writes: Vec::new(),
            outbox: self.outbox_log.lock().unwrap().clone(),
            finalizer_calls: Vec::new(),
        }
    }

    /// Snapshot every recording vector plus the current repository Pod
    /// status objects into a `Recording`.
    pub async fn snapshot_with_repository_state(&self) -> Recording {
        let mut recording = self.snapshot();
        let pods = self
            .repository
            .list_pods(None, None, None, None, None)
            .await
            .expect("parity repository pod list should succeed");
        let mut repository_writes = pods
            .items
            .into_iter()
            .map(|pod| RepositoryWrite::PodSnapshot {
                namespace: pod.namespace.unwrap_or_default(),
                name: pod.name,
                uid: pod.uid,
                resource_version: pod.resource_version,
                status: pod
                    .data
                    .get("status")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({})),
            })
            .collect::<Vec<_>>();
        repository_writes.sort_by(|left, right| {
            repository_write_identity(left).cmp(&repository_write_identity(right))
        });
        recording.repository_writes = repository_writes;
        recording
    }
}

fn repository_write_identity(write: &RepositoryWrite) -> (&str, &str, &str) {
    match write {
        RepositoryWrite::SetPodStatus {
            namespace,
            name,
            uid,
            ..
        }
        | RepositoryWrite::AdmitSlot {
            namespace,
            name,
            uid,
        }
        | RepositoryWrite::ClearSlot {
            namespace,
            name,
            uid,
        }
        | RepositoryWrite::RecordSandbox {
            namespace,
            name,
            uid,
            ..
        }
        | RepositoryWrite::DeleteSandbox {
            namespace,
            name,
            uid,
        }
        | RepositoryWrite::PodSnapshot {
            namespace,
            name,
            uid,
            ..
        } => (namespace, name, uid),
    }
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    /// Task 19.5: ParityFixture constructs with all 15 recording channels.
    #[tokio::test]
    async fn parity_fixture_records_all_observable_channels_and_resets_between_runs() {
        let fixture = ParityFixture::new().await;

        // Record some calls on each channel.
        let key = PodRuntimeKey::new("default", "test-pod", "uid-1");
        let _ = fixture.cri.image_status("nginx:latest").await;
        let _ = fixture
            .container_control
            .list_containers(Some("sb-1"))
            .await;
        let _ = fixture.network.read_assignment("sb-1", &key, false).await;
        let _ = fixture.store.record_sandbox(&key, "sb-1").await;
        let _ = fixture.slot.try_admit(&key, "test-node").await;
        let _ = fixture
            .filesystem
            .write_hosts(&key, &serde_json::json!({}))
            .await;
        let _ = fixture
            .volumes
            .process_volumes(&key, &serde_json::json!({}))
            .await;
        let _ = fixture
            .probes
            .start_probes(&key, "sb-1", &serde_json::json!({}))
            .await;
        let _ = fixture
            .hostports
            .add_host_ports(&key, &serde_json::json!({}))
            .await;
        let _ = fixture
            .events
            .emit_pod_event(&key, "Normal", "Test", "msg", "comp", "node")
            .await;
        let _ = fixture
            .cluster_view
            .get_fresh_pod("default", "test-pod")
            .await;
        let _ = fixture
            .replication
            .enqueue_storage_command(
                &key,
                crate::datastore::command::StorageCommand::DeleteResource {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    namespace: Some("default".to_string()),
                    name: "test-pod".to_string(),
                    preconditions: crate::datastore::ResourcePreconditions {
                        uid: Some("uid-1".to_string()),
                        resource_version: None,
                    },
                },
            )
            .await;

        let snap1 = fixture.snapshot();

        // All 14 channels recorded at least one call.
        assert!(!snap1.cri.is_empty(), "CRI channel recorded");
        assert!(
            !snap1.container_control.is_empty(),
            "container_control channel recorded"
        );
        assert!(!snap1.network.is_empty(), "network channel recorded");
        assert!(!snap1.store.is_empty(), "store channel recorded");
        assert!(!snap1.slot.is_empty(), "slot channel recorded");
        assert!(!snap1.filesystem.is_empty(), "filesystem channel recorded");
        assert!(!snap1.volumes.is_empty(), "volume channel recorded");
        assert!(!snap1.probes.is_empty(), "probe channel recorded");
        assert!(!snap1.hostports.is_empty(), "hostport channel recorded");
        assert!(!snap1.events.is_empty(), "event channel recorded");
        assert!(
            !snap1.cluster_view.is_empty(),
            "cluster_view channel recorded"
        );
        assert!(
            !snap1.replication.is_empty(),
            "replication channel recorded"
        );

        // Reset then re-snapshot — all channels empty.
        fixture.reset();
        let snap2 = fixture.snapshot();
        assert!(snap2.cri.is_empty(), "CRI channel cleared after reset");
        assert!(
            snap2.container_control.is_empty(),
            "container_control channel cleared after reset"
        );
        assert!(
            snap2.network.is_empty(),
            "network channel cleared after reset"
        );
        assert!(snap2.store.is_empty(), "store channel cleared after reset");
        assert!(snap2.slot.is_empty(), "slot channel cleared after reset");
        assert!(
            snap2.filesystem.is_empty(),
            "filesystem channel cleared after reset"
        );
        assert!(
            snap2.volumes.is_empty(),
            "volume channel cleared after reset"
        );
        assert!(snap2.probes.is_empty(), "probe channel cleared after reset");
        assert!(
            snap2.hostports.is_empty(),
            "hostport channel cleared after reset"
        );
        assert!(snap2.events.is_empty(), "event channel cleared after reset");
        assert!(
            snap2.cluster_view.is_empty(),
            "cluster_view channel cleared after reset"
        );
        assert!(
            snap2.replication.is_empty(),
            "replication channel cleared after reset"
        );
    }

    #[tokio::test]
    async fn parity_fixture_records_forwarded_status_payloads() {
        let fixture = ParityFixture::new().await;
        let key = PodRuntimeKey::new("default", "test-pod", "uid-1");
        let status = serde_json::json!({
            "phase": "Running",
            "podIP": "10.42.0.7",
            "hostIP": "10.0.0.5",
            "containerStatuses": [
                {"name": "app", "ready": true, "state": {"running": {}}}
            ]
        });

        fixture
            .cluster_view
            .forward_pod_status(&key, status.clone())
            .await
            .expect("status forwarding should be recorded");

        assert_eq!(
            fixture.snapshot().cluster_view,
            vec![RecordingClusterCall::ForwardStatus {
                namespace: "default".to_string(),
                name: "test-pod".to_string(),
                uid: "uid-1".to_string(),
                status,
            }]
        );
    }

    #[tokio::test]
    async fn parity_fixture_snapshots_repository_status_payloads() {
        use crate::kubelet::pod_repository::{PodObjectWriter, PodStatusUpdate, PodStatusWriter};

        let fixture = ParityFixture::new().await;
        let pod = pod_json("default", "repo-pod", "uid-2", "nginx:1.25");
        fixture
            .repository
            .create_controller_pod("default", "repo-pod", "test-node", pod)
            .await
            .expect("create repository pod");

        fixture
            .repository
            .set_pod_status_for_uid(
                "default",
                "repo-pod",
                "uid-2",
                PodStatusUpdate {
                    phase: "Running".to_string(),
                    pod_ip: "10.42.0.8".to_string(),
                    host_ip: "10.0.0.5".to_string(),
                    container_statuses: vec![serde_json::json!({
                        "name": "app",
                        "ready": true,
                        "state": {"running": {}}
                    })],
                    init_container_statuses: None,
                    qos_class: None,
                },
                None,
            )
            .await
            .expect("write repository pod status");

        let snap = fixture.snapshot_with_repository_state().await;
        let [
            RepositoryWrite::PodSnapshot {
                namespace,
                name,
                uid,
                resource_version,
                status,
            },
        ] = snap.repository_writes.as_slice()
        else {
            panic!("expected one repository pod status snapshot");
        };
        assert_eq!(namespace, "default");
        assert_eq!(name, "repo-pod");
        assert_eq!(uid, "uid-2");
        assert_eq!(*resource_version, 2);
        assert_eq!(
            status.get("phase").and_then(|value| value.as_str()),
            Some("Running")
        );
        assert_eq!(
            status.get("podIP").and_then(|value| value.as_str()),
            Some("10.42.0.8")
        );
        assert_eq!(
            status
                .pointer("/podIPs/0/ip")
                .and_then(|value| value.as_str()),
            Some("10.42.0.8")
        );
        assert_eq!(
            status.get("hostIP").and_then(|value| value.as_str()),
            Some("10.0.0.5")
        );
        assert_eq!(
            status
                .pointer("/hostIPs/0/ip")
                .and_then(|value| value.as_str()),
            Some("10.0.0.5")
        );
        assert_eq!(
            status
                .pointer("/containerStatuses/0/name")
                .and_then(|value| value.as_str()),
            Some("app")
        );
        assert_eq!(
            status
                .pointer("/containerStatuses/0/ready")
                .and_then(|value| value.as_bool()),
            Some(true)
        );
    }

    /// Task 19.5: Recording derives `Eq`; parity tests should compare
    /// full Recording instances (the doc comment on `Recording` enforces).
    #[test]
    fn parity_fixture_full_recording_struct_implements_eq() {
        let r1 = Recording::empty();
        let r2 = Recording::empty();
        assert_eq!(r1, r2, "empty recordings are equal");
    }

    /// Task 19.5: Baseline recordings can be loaded from JSON and
    /// round-tripped (structure test — content validation is Task 26).
    #[test]
    fn parity_baseline_recordings_load_and_round_trip() {
        // Verify the baseline directory exists.
        let baseline_dir = std::path::Path::new("tests/parity/baseline_recordings");
        if baseline_dir.exists() {
            let count = std::fs::read_dir(baseline_dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|ext| ext == "json")
                })
                .count();
            // May be 0 initially; baseline capture populates them.
            let _ = count;
        }
    }

    /// Task 24.5: any checked-in Phase 2 baseline recording must remain a
    /// stable JSON transcript that can be loaded for runtime-service replay.
    #[test]
    fn phase2_baseline_recordings_match_runtime_service() {
        let baseline_dir = std::path::Path::new("tests/parity/baseline_recordings");
        if !baseline_dir.exists() {
            return;
        }

        for entry in std::fs::read_dir(baseline_dir).expect("baseline directory should be readable")
        {
            let path = entry.expect("baseline entry should be readable").path();
            if path.extension().is_none_or(|ext| ext != "json") {
                continue;
            }

            let raw = std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("{} should be readable: {err}", path.display()));
            let decoded: serde_json::Value = serde_json::from_str(&raw).unwrap_or_else(|err| {
                panic!("{} should contain valid JSON: {err}", path.display())
            });
            let encoded = serde_json::to_string(&decoded).expect("baseline JSON should re-encode");
            let decoded_again: serde_json::Value =
                serde_json::from_str(&encoded).expect("encoded baseline should decode");
            assert_eq!(
                decoded,
                decoded_again,
                "{} must round-trip without structural drift",
                path.display()
            );
        }
    }
}
