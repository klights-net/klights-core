//! Leader control stream extracted from runtime.rs (R3 refactor).
//!
//! T2 step 5: on stream failure, the reconnect loop cycles through all
//! configured leader endpoints (--leader) instead of retrying the same
//! fixed endpoint indefinitely. This lets a worker find a new leader
//! after a raft election without restart.

pub async fn start_worker_leader_control_stream(
    client: std::sync::Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    cancel: tokio_util::sync::CancellationToken,
) -> anyhow::Result<crate::task_supervisor::SupervisedJoinHandle<()>> {
    let supervisor_for_task = supervisor.clone();
    supervisor
        .spawn_async(
            crate::task_supervisor::TaskCategory::Network,
            "worker_leader_control_stream",
            async move {
                run_worker_leader_control_stream(client, supervisor_for_task, cancel).await;
            },
        )
        .await
}

async fn run_worker_leader_control_stream(
    client: std::sync::Arc<crate::replication::grpc::client::ReplicationGrpcClient>,
    supervisor: std::sync::Arc<crate::task_supervisor::TaskSupervisor>,
    cancel: tokio_util::sync::CancellationToken,
) {
    let mut observed_rv = 0_i64;
    let mut attempt = 0_u32;
    loop {
        if cancel.is_cancelled() {
            return;
        }

        match client.ensure_joined().await {
            Ok(_) => {
                tracing::info!("worker leader control stream connected");
                attempt = 0;
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        next = client.stream_next() => {
                            match next {
                                Ok(crate::replication::protocol::StreamItem::Entry(entry)) => {
                                    observed_rv = observed_rv.max(entry.meta.resource_version);
                                    if let Err(err) = client.ack(observed_rv).await {
                                        tracing::debug!(error = %err, "failed to ACK worker leader stream entry");
                                        break;
                                    }
                                }
                                Ok(crate::replication::protocol::StreamItem::Heartbeat { current_rv }) => {
                                    observed_rv = observed_rv.max(current_rv);
                                    if let Err(err) = client.ack(observed_rv).await {
                                        tracing::debug!(error = %err, "failed to ACK worker leader stream heartbeat");
                                        break;
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(error = %err, "worker leader control stream disconnected");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "failed to connect worker leader control stream");
            }
        }

        // T2 step 5: cycle to the next leader endpoint before
        // reconnecting so the worker can find the new leader after a
        // raft election without a restart.
        let old_endpoint = client.current_leader_endpoint();
        let next_endpoint = client.try_next_endpoint();
        if next_endpoint != old_endpoint {
            tracing::info!(
                old = %old_endpoint,
                new = %next_endpoint,
                "worker leader control stream: cycling leader endpoint"
            );
        }

        let delay = worker_control_stream_reconnect_delay(attempt);
        attempt = attempt.saturating_add(1);
        tokio::select! {
            _ = cancel.cancelled() => return,
            result = supervisor.sleep("worker_leader_control_stream_reconnect", delay) => {
                if let Err(err) = result {
                    tracing::warn!(error = %err, "worker leader control stream reconnect timer failed");
                    return;
                }
            }
        }
    }
}

fn worker_control_stream_reconnect_delay(attempt: u32) -> std::time::Duration {
    let shift = attempt.clamp(0, 5);
    let millis = 250_u64.saturating_mul(1_u64 << shift).min(5_000);
    std::time::Duration::from_millis(millis)
}

pub fn runtime_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}
