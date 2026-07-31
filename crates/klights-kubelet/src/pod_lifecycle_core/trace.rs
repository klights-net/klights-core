//! Shared lifecycle trace types — moved from `pod_lifecycle_actor/trace.rs`.

use std::collections::VecDeque;

use super::message::PodLifecycleKey;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleTraceEntry {
    pub key: PodLifecycleKey,
    pub event: &'static str,
    pub resource_version: Option<i64>,
    pub sandbox_id: Option<String>,
    pub detail: String,
}

impl LifecycleTraceEntry {
    pub fn new(
        key: PodLifecycleKey,
        event: &'static str,
        resource_version: Option<i64>,
        sandbox_id: Option<&str>,
        detail: &str,
    ) -> Self {
        Self {
            key,
            event,
            resource_version,
            sandbox_id: sandbox_id.map(str::to_string),
            detail: detail.to_string(),
        }
    }
}

pub struct LifecycleTraceRing {
    capacity: usize,
    entries: VecDeque<LifecycleTraceEntry>,
}

impl LifecycleTraceRing {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: VecDeque::new(),
        }
    }

    pub fn record(&mut self, entry: LifecycleTraceEntry) {
        while self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        tracing::debug!(
            namespace = %entry.key.namespace,
            pod = %entry.key.name,
            uid = %entry.key.uid,
            rv = ?entry.resource_version,
            sandbox_id = ?entry.sandbox_id,
            event = entry.event,
            detail = %entry.detail,
            "pod lifecycle trace"
        );
        self.entries.push_back(entry);
    }

    pub fn entries_for(&self, key: &PodLifecycleKey) -> Vec<LifecycleTraceEntry> {
        self.entries
            .iter()
            .filter(|entry| &entry.key == key)
            .cloned()
            .collect()
    }

    pub fn snapshot(&self) -> Vec<LifecycleTraceEntry> {
        self.entries.iter().cloned().collect()
    }
}
