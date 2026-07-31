#[derive(Clone, Copy, Debug)]
pub enum LifecycleEvent {
    WatchAdded,
    CniAssigned,
    RetryDue,
    WatchModified,
    CriStarted,
    CriStopped,
    StatusEcho,
    TransientNetworkWait,
}

#[derive(Clone, Copy, Debug)]
pub struct LifecycleOrderCase {
    pub name: &'static str,
    pub events: &'static [LifecycleEvent],
}

#[derive(Debug, Default)]
struct InMemoryDatastore {
    pub cni_assigned: bool,
    pub status_echoed: bool,
    pub cri_started: bool,
    pub watch_started: bool,
    pub transient_network_wait: bool,
}

#[derive(Debug, Default)]
struct PodLifecycleState {
    pub finalized: bool,
}

#[derive(Default)]
pub struct PodLifecycleHarness {
    datastore: InMemoryDatastore,
    state: PodLifecycleState,
    /// Number of times finalizers (probes + owner enqueue + endpoint reconcile)
    /// were executed.
    pub finalized_count: usize,
    /// Number of times probe manager was started.
    pub probe_started_count: usize,
    /// Number of times owner controllers were reconciled.
    pub owner_enqueue_count: usize,
}

impl PodLifecycleHarness {
    pub async fn new() -> Self {
        Self::default()
    }

    pub async fn run_case(&mut self, case: &LifecycleOrderCase) {
        self.reset_for_case();

        for &event in case.events {
            self.apply_event(event);
        }
    }

    fn reset_for_case(&mut self) {
        self.datastore = InMemoryDatastore::default();
        self.state = PodLifecycleState::default();
        self.finalized_count = 0;
        self.probe_started_count = 0;
        self.owner_enqueue_count = 0;
    }

    fn apply_event(&mut self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::WatchAdded | LifecycleEvent::WatchModified => {
                self.datastore.watch_started = true;
            }
            LifecycleEvent::CniAssigned => {
                self.datastore.cni_assigned = true;
            }
            LifecycleEvent::RetryDue => {}
            LifecycleEvent::CriStarted => {
                self.datastore.cri_started = true;
            }
            LifecycleEvent::CriStopped => {}
            LifecycleEvent::StatusEcho => {
                self.datastore.status_echoed = true;
            }
            LifecycleEvent::TransientNetworkWait => {
                self.datastore.transient_network_wait = true;
            }
        }

        self.try_finalize();
    }

    fn try_finalize(&mut self) {
        if self.state.finalized || !self.datastore.watch_started {
            return;
        }

        // Transient network wait should not suppress startup if we already have a
        // direct readiness signal. This models the kubelet’s real behavior when
        // network assignment is delayed but eventual CRI/watch updates arrive.
        if self.datastore.transient_network_wait
            && !self.datastore.cni_assigned
            && !self.datastore.status_echoed
            && !self.datastore.cri_started
        {
            return;
        }

        if !self.has_ready_runtime_signal() {
            return;
        }

        self.state.finalized = true;
        self.finalized_count += 1;
        self.probe_started_count += 1;
        self.owner_enqueue_count += 1;
    }

    fn has_ready_runtime_signal(&self) -> bool {
        self.datastore.cni_assigned || self.datastore.status_echoed || self.datastore.cri_started
    }
}
