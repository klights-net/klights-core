//! Process time effects shared by graph-independent runtime owners.

/// Focused wall-clock effect for policy that needs deterministic tests.
pub trait WallClock: Send + Sync {
    fn now(&self) -> std::time::SystemTime;

    fn now_utc(&self) -> chrono::DateTime<chrono::Utc> {
        self.now().into()
    }
}

/// Host wall-clock implementation.
///
/// Composition code should inject this through [`WallClock`] when time affects
/// policy. Boundary code that only stamps an observation may use the static
/// value methods without reaching a feature or root-owned clock module.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemWallClock;

impl SystemWallClock {
    pub fn now() -> std::time::SystemTime {
        std::time::SystemTime::now()
    }

    pub fn now_utc() -> chrono::DateTime<chrono::Utc> {
        Self::now().into()
    }
}

impl WallClock for SystemWallClock {
    fn now(&self) -> std::time::SystemTime {
        Self::now()
    }
}

#[cfg(test)]
mod tests {
    use super::{SystemWallClock, WallClock};

    struct FixedClock(std::time::SystemTime);

    impl WallClock for FixedClock {
        fn now(&self) -> std::time::SystemTime {
            self.0
        }
    }

    #[test]
    fn injected_clock_projects_the_same_utc_instant() {
        let time = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_704_164_645);
        let clock = FixedClock(time);

        assert_eq!(clock.now(), time);
        assert_eq!(clock.now_utc().to_rfc3339(), "2024-01-02T03:04:05+00:00");
    }

    #[test]
    fn system_clock_returns_a_system_and_utc_value() {
        let before = std::time::SystemTime::now();
        let observed = SystemWallClock::now();
        let after = std::time::SystemTime::now();

        assert!(observed >= before);
        assert!(observed <= after);
        let _: chrono::DateTime<chrono::Utc> = SystemWallClock::now_utc();
    }
}
