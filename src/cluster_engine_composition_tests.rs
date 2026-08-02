use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::cluster_engine::{
    ClusterEngineConfigError, EngineNotImplemented, known_engine_names, run_selected,
    select_from_reader,
};

struct ClusterEngineEnvGuard {
    previous: Option<std::ffi::OsString>,
}

impl ClusterEngineEnvGuard {
    fn set(value: &str) -> Self {
        let previous = std::env::var_os("KLIGHTS_CLUSTER_ENGINE");
        // SAFETY: these tests serialize access with TEST_ENV_LOCK and restore
        // the prior value when the guard is dropped.
        unsafe { std::env::set_var("KLIGHTS_CLUSTER_ENGINE", value) };
        Self { previous }
    }

    fn remove() -> Self {
        let previous = std::env::var_os("KLIGHTS_CLUSTER_ENGINE");
        // SAFETY: these tests serialize access with TEST_ENV_LOCK and restore
        // the prior value when the guard is dropped.
        unsafe { std::env::remove_var("KLIGHTS_CLUSTER_ENGINE") };
        Self { previous }
    }
}

impl Drop for ClusterEngineEnvGuard {
    fn drop(&mut self) {
        // SAFETY: these tests serialize access with TEST_ENV_LOCK and this
        // restores the environment state captured by the guard.
        unsafe {
            if let Some(previous) = &self.previous {
                std::env::set_var("KLIGHTS_CLUSTER_ENGINE", previous);
            } else {
                std::env::remove_var("KLIGHTS_CLUSTER_ENGINE");
            }
        }
    }
}

#[test]
fn root_registry_contains_only_embedded_and_reserved_tikv_names() {
    assert_eq!(known_engine_names(), ["embedded", "tikv"]);
}

#[test]
fn root_selection_reads_cluster_engine_environment_exactly_once() {
    let reads = Cell::new(0);
    let selected = select_from_reader(|| {
        reads.set(reads.get() + 1);
        Ok("embedded".to_string())
    })
    .expect("embedded engine must be selected");

    assert!(selected.is_embedded());
    assert_eq!(reads.get(), 1);
}

#[test]
fn non_unicode_engine_value_is_a_typed_configuration_error() {
    use std::os::unix::ffi::OsStringExt;

    let error = select_from_reader(|| {
        Err(std::env::VarError::NotUnicode(
            std::ffi::OsString::from_vec(vec![0xff]),
        ))
    });
    let error = match error {
        Err(error) => error,
        Ok(_) => panic!("non-Unicode engine selection must fail validation"),
    };

    assert!(matches!(
        error.downcast_ref::<ClusterEngineConfigError>(),
        Some(ClusterEngineConfigError::NotUnicode)
    ));
}

#[test]
fn absent_engine_selection_preserves_the_embedded_default() {
    let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _env = ClusterEngineEnvGuard::remove();
    let embedded_starts = AtomicUsize::new(0);

    run_selected(|| {
        embedded_starts.fetch_add(1, Ordering::SeqCst);
    })
    .expect("the absent option must run the embedded graph");

    assert_eq!(embedded_starts.load(Ordering::SeqCst), 1);
}

#[test]
fn explicit_embedded_selection_preserves_the_embedded_graph() {
    let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _env = ClusterEngineEnvGuard::set("embedded");
    let embedded_starts = AtomicUsize::new(0);

    run_selected(|| {
        embedded_starts.fetch_add(1, Ordering::SeqCst);
    })
    .expect("the embedded option must run the existing graph");

    assert_eq!(embedded_starts.load(Ordering::SeqCst), 1);
}

#[test]
fn reserved_tikv_refuses_before_listeners_stores_or_raft_start() {
    let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _env = ClusterEngineEnvGuard::set("tikv");
    let listener_starts = AtomicUsize::new(0);
    let store_opens = AtomicUsize::new(0);
    let raft_starts = AtomicUsize::new(0);

    let error = run_selected(|| {
        listener_starts.fetch_add(1, Ordering::SeqCst);
        store_opens.fetch_add(1, Ordering::SeqCst);
        raft_starts.fetch_add(1, Ordering::SeqCst);
    })
    .expect_err("an adapter-less registered engine must fail closed");

    let refusal = error
        .downcast_ref::<EngineNotImplemented>()
        .expect("startup refusal must retain its typed error");
    assert_eq!(refusal.engine_name(), "tikv");
    assert_eq!(listener_starts.load(Ordering::SeqCst), 0);
    assert_eq!(store_opens.load(Ordering::SeqCst), 0);
    assert_eq!(raft_starts.load(Ordering::SeqCst), 0);
}

#[test]
fn unknown_engine_name_is_a_typed_configuration_error() {
    let _lock = crate::TEST_ENV_LOCK.lock().unwrap();
    let _env = ClusterEngineEnvGuard::set("unknown-engine");
    let embedded_starts = AtomicUsize::new(0);

    let error = run_selected(|| {
        embedded_starts.fetch_add(1, Ordering::SeqCst);
    })
    .expect_err("unknown names must fail configuration validation");

    assert!(matches!(
        error.downcast_ref::<ClusterEngineConfigError>(),
        Some(ClusterEngineConfigError::UnknownName { name }) if name == "unknown-engine"
    ));
    assert_eq!(embedded_starts.load(Ordering::SeqCst), 0);
}
