use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::Mutex;

pub struct SnapshotCache<K, T> {
    ttl: Duration,
    inner: Mutex<Option<CachedSnapshot<K, T>>>,
}

struct CachedSnapshot<K, T> {
    key: K,
    generated_at: Instant,
    value: T,
}

impl<K, T> SnapshotCache<K, T>
where
    K: Clone + PartialEq,
    T: Clone,
{
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Mutex::new(None),
        }
    }

    pub async fn get_or_generate<F, Fut>(&self, key: K, generate: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut guard = self.inner.lock().await;
        if let Some(cached) = guard.as_ref()
            && cached.key == key
            && cached.generated_at.elapsed() < self.ttl
        {
            return Ok(cached.value.clone());
        }

        let value = generate().await?;
        *guard = Some(CachedSnapshot {
            key,
            generated_at: Instant::now(),
            value: value.clone(),
        });
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::replication::grpc::snapshot_cache::SnapshotCache;

    #[tokio::test]
    async fn snapshot_cache_reuses_entries_within_ttl_and_same_rv() {
        let cache = SnapshotCache::new(Duration::from_secs(30));
        let first = cache
            .get_or_generate(10, || async { Ok::<_, anyhow::Error>(vec![1, 2, 3]) })
            .await
            .unwrap();
        let second = cache
            .get_or_generate(10, || async { Ok::<_, anyhow::Error>(vec![9]) })
            .await
            .unwrap();
        assert_eq!(first, vec![1, 2, 3]);
        assert_eq!(second, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn snapshot_cache_invalidates_when_rv_advances() {
        let cache = SnapshotCache::new(Duration::from_secs(30));
        cache
            .get_or_generate(10, || async { Ok::<_, anyhow::Error>(vec![1]) })
            .await
            .unwrap();
        let second = cache
            .get_or_generate(11, || async { Ok::<_, anyhow::Error>(vec![2]) })
            .await
            .unwrap();
        assert_eq!(second, vec![2]);
    }
}
