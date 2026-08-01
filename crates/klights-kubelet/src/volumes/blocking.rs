use anyhow::{Context, Result};
#[cfg(any(test, feature = "test-support"))]
use std::collections::HashMap;
#[cfg(any(test, feature = "test-support"))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(any(test, feature = "test-support"))]
static FILE_BLOCKING_KEYED_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(any(test, feature = "test-support"))]
static FILE_BLOCKING_KEYED_CALLS_BY_KEY: std::sync::OnceLock<
    std::sync::Mutex<HashMap<String, usize>>,
> = std::sync::OnceLock::new();

pub async fn run_blocking_fs_keyed<T>(
    file_process: &klights_supervisor::FileProcessExecutor,
    label: &'static str,
    key: &str,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    #[cfg(any(test, feature = "test-support"))]
    FILE_BLOCKING_KEYED_CALLS.fetch_add(1, Ordering::SeqCst);
    #[cfg(any(test, feature = "test-support"))]
    {
        let counters =
            FILE_BLOCKING_KEYED_CALLS_BY_KEY.get_or_init(|| std::sync::Mutex::new(HashMap::new()));
        let mut guard = counters
            .lock()
            .expect("file keyed call counter map poisoned");
        *guard.entry(format!("{label}\0{key}")).or_insert(0) += 1;
    }
    file_process
        .run_blocking_file_keyed(label, key, f)
        .await
        .with_context(|| format!("blocking keyed fs task '{label}' failed"))
}

#[cfg(any(test, feature = "test-support"))]
pub fn blocking_fs_keyed_call_count() -> usize {
    FILE_BLOCKING_KEYED_CALLS.load(Ordering::SeqCst)
}

#[cfg(any(test, feature = "test-support"))]
pub fn blocking_fs_keyed_call_count_for(label: &str, key: &str) -> usize {
    let Some(counters) = FILE_BLOCKING_KEYED_CALLS_BY_KEY.get() else {
        return 0;
    };
    let guard = counters
        .lock()
        .expect("file keyed call counter map poisoned");
    guard.get(&format!("{label}\0{key}")).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn keyed_blocking_fs_serializes_same_key() {
        let barrier = Arc::new(Barrier::new(2));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let file_process = crate::phase15d_test_support::file_process_executor();

        let run_one = |file_process: klights_supervisor::FileProcessExecutor,
                       barrier: Arc<Barrier>,
                       active: Arc<AtomicUsize>,
                       max_active: Arc<AtomicUsize>| async move {
            run_blocking_fs_keyed(&file_process, "keyed-fs-test", "volume/same", move || {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                let mut prev = max_active.load(Ordering::SeqCst);
                while now > prev
                    && max_active
                        .compare_exchange(prev, now, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                {
                    prev = max_active.load(Ordering::SeqCst);
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
                active.fetch_sub(1, Ordering::SeqCst);
                Ok::<(), anyhow::Error>(())
            })
            .await
            .unwrap();
            barrier.wait().await;
        };

        let t1 = tokio::spawn(run_one(
            file_process.clone(),
            barrier.clone(),
            active.clone(),
            max_active.clone(),
        ));
        let t2 = tokio::spawn(run_one(file_process, barrier, active, max_active.clone()));

        t1.await.unwrap();
        t2.await.unwrap();
        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "same-key filesystem tasks must not overlap"
        );
    }
}
