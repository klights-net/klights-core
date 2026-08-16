use std::sync::Arc;

use anyhow::{Result, anyhow};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Eq, PartialEq)]
enum CniReadinessState {
    Pending,
    Ready,
    Failed(Arc<str>),
}

#[derive(Clone)]
pub struct CniReadiness {
    rx: watch::Receiver<CniReadinessState>,
}

#[derive(Clone)]
pub struct CniReadinessPublisher {
    tx: watch::Sender<CniReadinessState>,
}

impl CniReadiness {
    pub fn channel() -> (CniReadinessPublisher, Self) {
        let (tx, rx) = watch::channel(CniReadinessState::Pending);
        (CniReadinessPublisher { tx }, Self { rx })
    }

    pub async fn wait_ready(&self, cancel: CancellationToken) -> Result<()> {
        let mut rx = self.rx.clone();
        loop {
            match &*rx.borrow() {
                CniReadinessState::Ready => return Ok(()),
                CniReadinessState::Failed(message) => {
                    return Err(anyhow!("CNI readiness failed: {message}"));
                }
                CniReadinessState::Pending => {}
            }

            tokio::select! {
                _ = cancel.cancelled() => {
                    return Err(anyhow!("cancelled while waiting for CNI readiness"));
                }
                changed = rx.changed() => {
                    changed.map_err(|_| anyhow!("CNI readiness publisher closed before readiness"))?;
                }
            }
        }
    }
}

impl CniReadinessPublisher {
    pub fn publish_ready(&self) {
        let _ = self.tx.send(CniReadinessState::Ready);
    }

    pub fn publish_failed(&self, message: impl Into<Arc<str>>) {
        let _ = self.tx.send(CniReadinessState::Failed(message.into()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn published_readiness_is_retained_for_late_consumers() {
        let (publisher, readiness) = CniReadiness::channel();
        publisher.publish_ready();

        readiness
            .wait_ready(CancellationToken::new())
            .await
            .expect("late consumers must observe retained readiness");
    }

    #[tokio::test]
    async fn wait_blocks_until_readiness_is_published() {
        let (publisher, readiness) = CniReadiness::channel();
        let cancel = CancellationToken::new();
        let waiter = tokio::spawn(async move { readiness.wait_ready(cancel).await });

        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "readiness wait must park without timer or CRI probes"
        );

        publisher.publish_ready();
        waiter
            .await
            .expect("wait task must complete")
            .expect("ready publication should release waiter");
    }

    #[tokio::test]
    async fn wait_returns_failure_and_cancellation() {
        let (publisher, failed_readiness) = CniReadiness::channel();
        publisher.publish_failed("runtime unavailable");
        let err = failed_readiness
            .wait_ready(CancellationToken::new())
            .await
            .expect_err("published failure must be terminal");
        assert!(err.to_string().contains("runtime unavailable"));

        let (_publisher, cancelled_readiness) = CniReadiness::channel();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = cancelled_readiness
            .wait_ready(cancel)
            .await
            .expect_err("cancelled wait must terminate");
        assert!(err.to_string().contains("cancelled"));
    }
}
