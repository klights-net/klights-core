use std::time::Instant;
use time::OffsetDateTime;

/// Object-safe time source for auth code that must be unit-testable without
/// depending on the host wall clock.
pub trait Clock: Send + Sync {
    fn now(&self) -> OffsetDateTime;
}

pub fn chrono_utc(now: OffsetDateTime) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from(std::time::SystemTime::from(now))
}

/// Production wall-clock source.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> OffsetDateTime {
        time::OffsetDateTime::from(klights_supervisor::SystemWallClock::now())
    }
}

/// Operation-scoped wall-clock snapshot.
///
/// Policy code can pass this through interfaces that accept [`Clock`] while
/// guaranteeing every read in that operation observes the same instant.
pub(crate) struct SnapshotClock {
    now: OffsetDateTime,
}

impl SnapshotClock {
    pub(crate) fn new(now: OffsetDateTime) -> Self {
        Self { now }
    }
}

impl Clock for SnapshotClock {
    fn now(&self) -> OffsetDateTime {
        self.now
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
