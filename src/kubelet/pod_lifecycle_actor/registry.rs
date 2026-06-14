use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::kubelet::pod_lifecycle_router::LifecycleReplyHandle;
use crate::kubelet::pod_lifecycle_router::executor::PodWorkExecutor;
use crate::task_supervisor::{TaskCategory, TaskSupervisor};

use super::actor::{PodLifecycleActor, PodLifecycleActorRuntime, pod_actor_idle_grace_duration};
use super::config::PodLifecycleConcurrencyConfig;
use super::message::{LifecycleMessage, PodLifecycleKey, PodSlotKey};
use super::trace::{LifecycleTraceEntry, LifecycleTraceRing};

/// Capacity for the per-pod actor mailbox channel.
const ACTOR_MAILBOX_CAPACITY: usize = 256;

#[derive(Clone)]
pub struct LifecycleSender {
    tx: mpsc::Sender<LifecycleMessage>,
}

impl LifecycleSender {
    pub async fn send(
        &self,
        message: LifecycleMessage,
    ) -> Result<(), mpsc::error::SendError<LifecycleMessage>> {
        self.tx.send(message).await
    }

    pub fn try_send_nonblocking(&self, message: LifecycleMessage) {
        let _ = self.tx.try_send(message);
    }

    pub fn same_channel(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }
}

impl fmt::Debug for LifecycleSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LifecycleSender").finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct ActorInstanceToken(Arc<()>);

impl ActorInstanceToken {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    fn same_instance(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl fmt::Debug for ActorInstanceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ActorInstanceToken").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct ActorEntry {
    sender: LifecycleSender,
    shutdown_token: CancellationToken,
    instance: ActorInstanceToken,
}

#[derive(Clone)]
pub struct PodLifecycleActorRemovalHandle {
    actors: Arc<Mutex<HashMap<PodSlotKey, ActorEntry>>>,
    actor_states: Arc<Mutex<HashMap<PodSlotKey, PodLifecycleActorStateEntry>>>,
}

impl PodLifecycleActorRemovalHandle {
    pub async fn try_remove_if_idle(
        &self,
        slot: &PodSlotKey,
        expected_instance: &ActorInstanceToken,
    ) -> bool {
        let removed = {
            let mut actors = self.actors.lock().await;
            if actors
                .get(slot)
                .is_some_and(|entry| entry.instance.same_instance(expected_instance))
            {
                actors.remove(slot)
            } else {
                None
            }
        };

        if let Some(entry) = removed {
            entry.shutdown_token.cancel();
            self.actor_states.lock().await.remove(slot);
            return true;
        }

        false
    }
}

#[derive(Debug)]
pub enum PodLifecycleRegistryError {
    SpawnFailed(anyhow::Error),
}

impl fmt::Display for PodLifecycleRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpawnFailed(err) => write!(f, "failed to spawn pod lifecycle actor: {err:#}"),
        }
    }
}

impl std::error::Error for PodLifecycleRegistryError {}

pub struct PodLifecycleRegistry {
    supervisor: Arc<TaskSupervisor>,
    _config: PodLifecycleConcurrencyConfig,
    actors: Arc<Mutex<HashMap<PodSlotKey, ActorEntry>>>,
    actor_states: Arc<Mutex<HashMap<PodSlotKey, PodLifecycleActorStateEntry>>>,
    trace: Arc<Mutex<LifecycleTraceRing>>,
    executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
    reply_handle: std::sync::Mutex<Option<LifecycleReplyHandle>>,
    idle_grace: Duration,
}

#[derive(Clone, Debug)]
pub struct PodLifecycleActorStateEntry {
    pub uid: String,
    pub state: String,
}

#[derive(Clone, Debug)]
pub struct PodLifecycleActorState {
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub state: String,
}

impl PodLifecycleRegistry {
    pub fn new(
        supervisor: Arc<TaskSupervisor>,
        config: PodLifecycleConcurrencyConfig,
        executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
    ) -> Self {
        Self::new_with_idle_grace(
            supervisor,
            config,
            executor_holder,
            pod_actor_idle_grace_duration(),
        )
    }

    fn new_with_idle_grace(
        supervisor: Arc<TaskSupervisor>,
        config: PodLifecycleConcurrencyConfig,
        executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
        idle_grace: Duration,
    ) -> Self {
        Self {
            supervisor,
            _config: config.normalized(),
            actors: Arc::new(Mutex::new(HashMap::new())),
            actor_states: Arc::new(Mutex::new(HashMap::new())),
            trace: Arc::new(Mutex::new(LifecycleTraceRing::new(256))),
            executor_holder,
            reply_handle: std::sync::Mutex::new(None),
            idle_grace,
        }
    }

    #[cfg(test)]
    pub fn new_with_idle_grace_for_test(
        supervisor: Arc<TaskSupervisor>,
        config: PodLifecycleConcurrencyConfig,
        executor_holder: Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>>,
        idle_grace: Duration,
    ) -> Self {
        Self::new_with_idle_grace(supervisor, config, executor_holder, idle_grace)
    }

    /// Set the reply handle after the router is constructed (circular dep).
    pub fn set_reply_handle(&self, handle: LifecycleReplyHandle) {
        *self.reply_handle.lock().unwrap() = Some(handle);
    }

    pub fn executor_holder(&self) -> Arc<std::sync::Mutex<Arc<dyn PodWorkExecutor>>> {
        self.executor_holder.clone()
    }

    pub async fn sender_for(
        &self,
        key: PodLifecycleKey,
    ) -> Result<LifecycleSender, PodLifecycleRegistryError> {
        let slot = PodSlotKey::from(&key);
        let mut actors = self.actors.lock().await;
        if let Some(entry) = actors.get(&slot) {
            return Ok(entry.sender.clone());
        }

        let (tx, rx) = mpsc::channel::<LifecycleMessage>(ACTOR_MAILBOX_CAPACITY);
        let sender = LifecycleSender { tx };
        let shutdown_token = self.supervisor.root_cancellation_token().child_token();
        let instance = ActorInstanceToken::new();
        actors.insert(
            slot.clone(),
            ActorEntry {
                sender: sender.clone(),
                shutdown_token: shutdown_token.clone(),
                instance: instance.clone(),
            },
        );
        {
            let mut actor_states = self.actor_states.lock().await;
            actor_states.insert(
                slot.clone(),
                PodLifecycleActorStateEntry {
                    uid: key.uid.clone(),
                    state: "running".to_string(),
                },
            );
        }
        drop(actors);

        let reply_handle = self
            .reply_handle
            .lock()
            .unwrap()
            .clone()
            .expect("reply_handle must be set before first sender_for call");

        let spawn_result = self
            .supervisor
            .spawn_async(
                TaskCategory::PodLifecycleActor,
                "pod_lifecycle_actor",
                PodLifecycleActor::new_with_shared_trace_and_state(PodLifecycleActorRuntime {
                    slot: slot.clone(),
                    trace: self.trace.clone(),
                    actor_state: self.actor_states.clone(),
                    supervisor: self.supervisor.clone(),
                    executor_holder: self.executor_holder.clone(),
                    reply_handle,
                    self_removal: self.removal_handle(),
                    shutdown_token,
                    instance,
                    idle_grace: self.idle_grace,
                })
                .run(rx),
            )
            .await;

        if let Err(err) = spawn_result {
            let mut actors = self.actors.lock().await;
            if actors
                .get(&slot)
                .map(|existing| existing.sender.same_channel(&sender))
                .unwrap_or(false)
                && let Some(entry) = actors.remove(&slot)
            {
                entry.shutdown_token.cancel();
            }
            let mut actor_states = self.actor_states.lock().await;
            actor_states.remove(&slot);
            return Err(PodLifecycleRegistryError::SpawnFailed(anyhow::anyhow!(
                "spawn pod lifecycle actor for {}/{}/{}: {err:#}",
                key.namespace,
                key.name,
                key.uid
            )));
        }

        Ok(sender)
    }

    /// Nonblocking lookup of an existing sender. Returns `None` if the
    /// actor has not been created yet or the lock is held. Used by the
    /// router for fire-and-forget trace messages that must never block
    /// event processing.
    pub fn try_sender_for(&self, key: &PodLifecycleKey) -> Option<LifecycleSender> {
        let slot = PodSlotKey::from(key);
        self.actors
            .try_lock()
            .ok()?
            .get(&slot)
            .map(|entry| entry.sender.clone())
    }

    pub async fn actor_count(&self) -> usize {
        self.actors.lock().await.len()
    }

    fn removal_handle(&self) -> PodLifecycleActorRemovalHandle {
        PodLifecycleActorRemovalHandle {
            actors: self.actors.clone(),
            actor_states: self.actor_states.clone(),
        }
    }

    pub async fn try_remove_if_idle(
        &self,
        slot: &PodSlotKey,
        expected_instance: &ActorInstanceToken,
    ) -> bool {
        self.removal_handle()
            .try_remove_if_idle(slot, expected_instance)
            .await
    }

    #[cfg(test)]
    pub async fn actor_instance_token_for_test(
        &self,
        key: &PodLifecycleKey,
    ) -> Option<ActorInstanceToken> {
        let slot = PodSlotKey::from(key);
        self.actors
            .lock()
            .await
            .get(&slot)
            .map(|entry| entry.instance.clone())
    }

    /// Remove the actor entry for a pod key. Drops the registry-owned sender,
    /// which closes the actor mailbox channel and lets the actor task exit.
    /// Idempotent: returns `true` if an entry was removed, `false` if the
    /// key was not registered.
    pub async fn remove_actor(&self, key: &PodLifecycleKey) -> bool {
        let slot = PodSlotKey::from(key);
        let removed = self.actors.lock().await.remove(&slot);
        if let Some(entry) = removed {
            entry.shutdown_token.cancel();
            self.actor_states.lock().await.remove(&slot);
            let remaining = self.actors.lock().await.len();
            tracing::debug!(
                namespace = %key.namespace,
                pod = %key.name,
                uid = %key.uid,
                remaining,
                "pod lifecycle actor removed from registry"
            );
            return true;
        }
        false
    }

    pub async fn actor_states_snapshot(&self) -> Vec<PodLifecycleActorState> {
        let mut states: Vec<_> = self
            .actor_states
            .lock()
            .await
            .iter()
            .map(|(slot, entry)| PodLifecycleActorState {
                namespace: slot.namespace.clone(),
                name: slot.name.clone(),
                uid: entry.uid.clone(),
                state: entry.state.clone(),
            })
            .collect();
        states.sort_by(|a, b| {
            (a.namespace.clone(), a.name.clone(), a.uid.clone()).cmp(&(
                b.namespace.clone(),
                b.name.clone(),
                b.uid.clone(),
            ))
        });
        states
    }

    pub async fn recent_trace(&self, limit: usize) -> Vec<LifecycleTraceEntry> {
        let trace = self.trace.lock().await;
        let snapshot = trace.snapshot();
        let len = snapshot.len();
        if len <= limit {
            snapshot
        } else {
            snapshot[len.saturating_sub(limit)..].to_vec()
        }
    }
}
