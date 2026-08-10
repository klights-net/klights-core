//! Private runtime-port parity recordings.

use std::sync::Arc;

use crate::pod_deletion_finalizer::PodDeletionFinalizer;
use crate::pod_env::EnvSourceReader;
use crate::runtime::PodRuntimeKey;
use crate::runtime::cri::{ContainerRuntimeControl, CriRuntime};
use crate::runtime::events::PodEventSink;
use crate::runtime::events::test_support::{MockPodEvent, MockPodEventSink};
use crate::runtime::filesystem::PodFilesystem;
use crate::runtime::filesystem::test_support::MockPodFilesystem;
use crate::runtime::hooks::PodHookRuntime;
use crate::runtime::hostports::HostPortRuntime;
use crate::runtime::hostports::test_support::{MockHostPortOp, MockHostPortRuntime};
use crate::runtime::network::PodNetworkRuntime;
use crate::runtime::network::test_support::{MockNetworkOp, MockPodNetworkRuntime};
use crate::runtime::probes::ProbeRuntime;
use crate::runtime::probes::test_support::{MockProbeCall, MockProbeRuntime};
use crate::runtime::store::{PodRuntimeStore, PodSlotAdmission};
use crate::runtime::test_support::{
    MockContainerControlOp, MockContainerRuntimeControl, MockCriCall, MockCriRuntime,
    MockEnvSourceReader, MockHookCall, MockPodDeletionFinalizer, MockPodHookRuntime,
    MockPodRuntimeStore, MockPodSlotAdmission,
};
use crate::runtime::volumes::PodVolumeRuntime;
use crate::runtime::volumes::test_support::MockPodVolumeRuntime;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Recording {
    cri: Vec<MockCriCall>,
    container_control: Vec<MockContainerControlOp>,
    network: Vec<MockNetworkOp>,
    store: Vec<String>,
    slot: Vec<String>,
    filesystem: Vec<String>,
    volumes: Vec<String>,
    probes: Vec<MockProbeCall>,
    hostports: Vec<MockHostPortOp>,
    events: Vec<MockPodEvent>,
    hooks: Vec<MockHookCall>,
    env: Vec<String>,
    finalizer_calls: Vec<PodRuntimeKey>,
}

impl Recording {
    fn empty() -> Self {
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
            hooks: Vec::new(),
            env: Vec::new(),
            finalizer_calls: Vec::new(),
        }
    }
}

struct RuntimeParityFixture {
    cri: Arc<MockCriRuntime>,
    container_control: Arc<MockContainerRuntimeControl>,
    network: Arc<MockPodNetworkRuntime>,
    store: Arc<MockPodRuntimeStore>,
    slot: Arc<MockPodSlotAdmission>,
    filesystem: Arc<MockPodFilesystem>,
    volumes: Arc<MockPodVolumeRuntime>,
    probes: Arc<MockProbeRuntime>,
    hostports: Arc<MockHostPortRuntime>,
    events: Arc<MockPodEventSink>,
    hooks: Arc<MockPodHookRuntime>,
    env: Arc<MockEnvSourceReader>,
    finalizer: Arc<MockPodDeletionFinalizer>,
}

impl RuntimeParityFixture {
    fn new() -> Self {
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
            hooks: Arc::new(MockPodHookRuntime::new()),
            env: Arc::new(MockEnvSourceReader::new()),
            finalizer: Arc::new(MockPodDeletionFinalizer::new()),
        }
    }

    fn snapshot(&self) -> Recording {
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
            hooks: self.hooks.recorded_calls(),
            env: self.env.recorded_calls(),
            finalizer_calls: self.finalizer.recorded_calls(),
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }
}

#[tokio::test]
async fn parity_fixture_records_all_observable_channels_and_resets_between_runs() {
    let mut fixture = RuntimeParityFixture::new();
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
        .add_host_ports(
            &klights_network_api::PodHostPorts::try_new(
                klights_types::PodIdentity::new(&key.namespace, &key.name, &key.uid),
                None,
                Vec::new(),
            )
            .unwrap(),
        )
        .await;
    let _ = fixture
        .events
        .emit_pod_event(&key, "Normal", "Test", "msg", "comp", "node")
        .await;
    let _ = fixture
        .hooks
        .execute_post_start(
            "container-1",
            "10.42.0.7",
            &serde_json::json!({}),
            &serde_json::json!({}),
        )
        .await;
    let _ = fixture.env.secret("default", "missing").await;
    let _ = fixture.finalizer.finalize_after_actor_cleanup(&key).await;

    let expected = Recording {
        cri: fixture.cri.recorded_calls(),
        container_control: fixture.container_control.recorded_calls(),
        network: fixture.network.recorded_calls(),
        store: fixture.store.recorded_calls(),
        slot: fixture.slot.recorded_calls(),
        filesystem: fixture.filesystem.recorded_calls(),
        volumes: fixture.volumes.recorded_calls(),
        probes: fixture.probes.recorded_calls(),
        hostports: fixture.hostports.recorded_calls(),
        events: fixture.events.recorded_events(),
        hooks: fixture.hooks.recorded_calls(),
        env: fixture.env.recorded_calls(),
        finalizer_calls: fixture.finalizer.recorded_calls(),
    };
    assert_eq!(fixture.snapshot(), expected);
    assert!(expected.cri.len() == 1 && expected.finalizer_calls == vec![key]);

    fixture.reset();
    assert_eq!(fixture.snapshot(), Recording::empty());
}

#[test]
fn parity_fixture_full_recording_struct_implements_eq() {
    let r1 = Recording::empty();
    let r2 = Recording::empty();
    assert_eq!(r1, r2, "empty recordings are equal");
}
