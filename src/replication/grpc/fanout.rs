use tokio::sync::mpsc;

pub struct FanoutPool<T> {
    batch_size: usize,
    followers: Vec<Follower<T>>,
}

struct Follower<T> {
    id: String,
    session_id: u64,
    sender: mpsc::Sender<T>,
}

impl<T> FanoutPool<T>
where
    T: Clone,
{
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            followers: Vec::new(),
        }
    }

    pub fn add_follower(&mut self, id: String, session_id: u64, sender: mpsc::Sender<T>) {
        self.followers.retain(|follower| follower.id != id);
        self.followers.push(Follower {
            id,
            session_id,
            sender,
        });
    }

    pub fn follower_count(&self) -> usize {
        self.followers.len()
    }

    pub fn worker_count(&self) -> usize {
        self.followers.len().div_ceil(self.batch_size)
    }

    /// Publish `item` to every follower.
    ///
    /// Returns a list of `(id, session_id)` for followers that must be
    /// disconnected:
    /// - **Closed:** receiver dropped (genuine disconnect).
    /// - **Full:** bounded queue is full. Replication entries are ordered and
    ///   lossless, so a follower that cannot accept the next item must
    ///   reconnect and catch up through a snapshot instead of silently
    ///   dropping the item while staying connected.
    pub fn publish(&mut self, item: T) -> Vec<(String, u64)> {
        let mut disconnected = Vec::new();
        self.followers
            .retain(|follower| match follower.sender.try_send(item.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    disconnected.push((follower.id.clone(), follower.session_id));
                    false
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    disconnected.push((follower.id.clone(), follower.session_id));
                    false
                }
            });
        disconnected
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::grpc::fanout::FanoutPool;

    #[test]
    fn fanout_distributes_followers_across_batches() {
        let mut pool = FanoutPool::<i32>::new(2);
        for (i, id) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            let (tx, _rx) = tokio::sync::mpsc::channel(4);
            pool.add_follower(id.to_string(), i as u64, tx);
        }
        assert_eq!(pool.worker_count(), 3);
        assert_eq!(pool.follower_count(), 5);
    }

    #[tokio::test]
    async fn full_follower_disconnects_without_blocking_other_followers() {
        let mut pool = FanoutPool::<i32>::new(50);
        let (slow_tx, mut slow_rx) = tokio::sync::mpsc::channel(1);
        slow_tx.try_send(1).unwrap(); // fill the slow channel
        let (fast_tx, mut fast_rx) = tokio::sync::mpsc::channel(1);
        pool.add_follower("slow".to_string(), 1, slow_tx);
        pool.add_follower("fast".to_string(), 2, fast_tx);

        let disconnected = pool.publish(2);
        assert_eq!(disconnected, vec![("slow".to_string(), 1)]);
        assert_eq!(fast_rx.recv().await, Some(2));
        assert_eq!(slow_rx.recv().await, Some(1));
        assert_eq!(slow_rx.recv().await, None);
        assert_eq!(pool.follower_count(), 1);
    }

    #[tokio::test]
    async fn full_follower_is_disconnected_before_any_replication_entry_is_dropped() {
        let mut pool = FanoutPool::<i32>::new(50);
        let (slow_tx, mut slow_rx) = tokio::sync::mpsc::channel(1);
        slow_tx.try_send(1).unwrap();
        let (fast_tx, mut fast_rx) = tokio::sync::mpsc::channel(1);

        pool.add_follower("slow".to_string(), 7, slow_tx);
        pool.add_follower("fast".to_string(), 8, fast_tx);

        let disconnected = pool.publish(2);

        assert_eq!(disconnected, vec![("slow".to_string(), 7)]);
        assert_eq!(fast_rx.recv().await, Some(2));
        assert_eq!(slow_rx.recv().await, Some(1));
        assert_eq!(slow_rx.recv().await, None);
        assert_eq!(pool.follower_count(), 1);
    }

    #[tokio::test]
    async fn closed_follower_is_removed() {
        let mut pool = FanoutPool::<i32>::new(50);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        pool.add_follower("drop-me".to_string(), 1, tx);
        drop(rx); // close the channel

        let disconnected = pool.publish(42);
        assert_eq!(disconnected, vec![("drop-me".to_string(), 1)]);
        assert_eq!(pool.follower_count(), 0);
    }

    /// A follower whose bounded queue is full must be evicted immediately so
    /// it can resync via snapshot catch-up before any replication entry is
    /// lost.
    #[tokio::test]
    async fn full_follower_is_evicted_immediately() {
        let mut pool = FanoutPool::<i32>::new(50);
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(0).unwrap();
        pool.add_follower("stuck".to_string(), 1, tx);

        let disconnected = pool.publish(1);

        assert_eq!(disconnected, vec![("stuck".to_string(), 1)]);
        assert_eq!(pool.follower_count(), 0);

        // The receiver should still be alive (we didn't drop it).
        drop(rx);
    }
}
