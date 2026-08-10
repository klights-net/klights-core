//! Compiler-first contract for the intentionally small P7.B public surface.

use klights_kubelet::pod_subsystem::{PodSubsystem, PodSubsystemConfig};
use klights_kubelet::runtime::PodRuntimeService;
use klights_kubelet::runtime::events::{PodEventSink, PodEventSinkError};

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn p7_b_exposes_only_focused_construction_and_event_input() {
    assert_send_sync::<PodSubsystem>();
    assert_send_sync::<PodSubsystemConfig>();
    assert_send_sync::<PodEventSinkError>();
    let _: Option<&dyn PodEventSink> = None;
    let _: Option<&dyn PodRuntimeService> = None;
}
