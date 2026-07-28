//! Reconnect policy for event streams.

/// Exponential reconnect delay from 500 ms, capped at 60 seconds.
pub fn delay(attempt: u32) -> std::time::Duration {
    const BASE_MS: u64 = 500;
    const MAX_MS: u64 = 60_000;
    let shift = attempt.min(7);
    std::time::Duration::from_millis((BASE_MS << shift).min(MAX_MS))
}

#[cfg(test)]
mod tests {
    use super::delay;
    use std::time::Duration;

    #[test]
    fn delay_is_exponential_and_capped() {
        assert_eq!(delay(0), Duration::from_millis(500));
        assert_eq!(delay(6), Duration::from_secs(32));
        assert_eq!(delay(7), Duration::from_secs(60));
        assert_eq!(delay(1000), Duration::from_secs(60));
    }
}
