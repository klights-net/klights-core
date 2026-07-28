//! Poison-tolerant locking for short, panic-safe bookkeeping sections.

/// Acquire a `std::sync::Mutex` guard, recovering silently from poison.
///
/// Use only for short sections whose protected value remains consistent if a
/// caller panics. State transitions that can be left half-applied must instead
/// be made panic-free.
#[must_use = "the returned MutexGuard must be bound or the lock is released immediately"]
pub fn lock_recover<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::lock_recover;
    use std::sync::Mutex;

    #[test]
    fn returns_inner_value_after_poison() {
        let mutex = Mutex::new(42_u32);
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().expect("first lock should succeed");
            panic!("intentional poison for test");
        }));
        assert!(unwind.is_err());
        assert!(mutex.lock().is_err());
        assert_eq!(*lock_recover(&mutex), 42);
    }
}
