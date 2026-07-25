use std::time::Instant;
use time::OffsetDateTime;

/// Object-safe time source for auth code that must be unit-testable without
/// depending on the host wall clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

/// Production wall-clock source.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// Object-safe monotonic time source for auth caches.
pub trait MonotonicClock: Send + Sync {
    fn now(&self) -> Instant;
}

/// Production monotonic clock source.
pub struct SystemMonotonicClock;

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Fixed clock for mock-backed auth tests.
#[cfg(test)]
pub struct FixedClock {
    pub now: OffsetDateTime,
}

#[cfg(test)]
impl Clock for FixedClock {
    fn now(&self) -> OffsetDateTime {
        self.now
    }
}
