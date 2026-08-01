/// Clock used by kubelet runtime, lifecycle, probe, and delivery work.
pub trait RuntimeClock: Send + Sync {
    fn now_ms(&self) -> i64;

    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.now_ms())
            .unwrap_or(chrono::DateTime::UNIX_EPOCH)
    }
}

pub struct SystemRuntimeClock;

impl RuntimeClock for SystemRuntimeClock {
    fn now_ms(&self) -> i64 {
        klights_supervisor::SystemWallClock::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64
    }
}

impl klights_supervisor::WallClock for SystemRuntimeClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_millis(self.now_ms().max(0) as u64)
    }
}
