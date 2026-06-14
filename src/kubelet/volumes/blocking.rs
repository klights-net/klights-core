use anyhow::{Context, Result};
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

static FILE_KEYED_LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
    OnceLock::new();
#[cfg(test)]
static FILE_BLOCKING_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FILE_BLOCKING_KEYED_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static FILE_BLOCKING_KEYED_CALLS_BY_KEY: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();

fn keyed_lock(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    let map = FILE_KEYED_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().expect("file keyed lock map poisoned");
    guard
        .entry(key.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
}

pub async fn run_blocking_fs<T>(
    label: &'static str,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    #[cfg(test)]
    FILE_BLOCKING_CALLS.fetch_add(1, Ordering::SeqCst);
    crate::kubelet::file_blocking::run_blocking_file(label, f)
        .await
        .with_context(|| format!("blocking fs task '{}' failed", label))
}

pub async fn run_blocking_fs_keyed<T>(
    label: &'static str,
    key: &str,
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    #[cfg(test)]
    FILE_BLOCKING_KEYED_CALLS.fetch_add(1, Ordering::SeqCst);
    #[cfg(test)]
    {
        let counters = FILE_BLOCKING_KEYED_CALLS_BY_KEY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut guard = counters
            .lock()
            .expect("file keyed call counter map poisoned");
        *guard.entry(format!("{label}\0{key}")).or_insert(0) += 1;
    }
    let lock = keyed_lock(key);
    let _guard = lock.lock().await;
    run_blocking_fs(label, f).await
}

#[cfg(test)]
pub fn blocking_fs_keyed_call_count() -> usize {
    FILE_BLOCKING_KEYED_CALLS.load(Ordering::SeqCst)
}

#[cfg(test)]
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

        let run_one = |barrier: Arc<Barrier>,
                       active: Arc<AtomicUsize>,
                       max_active: Arc<AtomicUsize>| async move {
            run_blocking_fs_keyed("keyed-fs-test", "volume/same", move || {
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

        let t1 = tokio::spawn(run_one(barrier.clone(), active.clone(), max_active.clone()));
        let t2 = tokio::spawn(run_one(barrier, active, max_active.clone()));

        t1.await.unwrap();
        t2.await.unwrap();
        assert_eq!(
            max_active.load(Ordering::SeqCst),
            1,
            "same-key filesystem tasks must not overlap"
        );
    }
}
