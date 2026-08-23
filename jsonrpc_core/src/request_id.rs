use serde::{Deserialize, Serialize};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};

/// A JSON-RPC request id. Wraps a `u64` rather than the full spec-permitted
/// `number | string | null`, since every id this crate ever produces is one
/// it allocated itself via [`RequestIdManager`].
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new(id: u64) -> Self {
        RequestId(id)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    fn from(id: u64) -> Self {
        RequestId(id)
    }
}

impl From<RequestId> for u64 {
    fn from(id: RequestId) -> Self {
        id.0
    }
}

/// Allocates unique, monotonically increasing [`RequestId`]s for a client.
/// Safe to share across threads/tasks — every method takes `&self`.
pub struct RequestIdManager {
    current_id: AtomicU64,
}

impl Default for RequestIdManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestIdManager {
    pub fn new() -> Self {
        Self {
            current_id: AtomicU64::new(0),
        }
    }
    /// Atomically reserves and returns the next id.
    pub fn reserve_id(&self) -> RequestId {
        RequestId::new(self.current_id.fetch_add(1, Ordering::Relaxed))
    }

    /// The next id that will be handed out by [`reserve_id`](Self::reserve_id)
    /// or [`reserve_range`](Self::reserve_range), without reserving it.
    pub fn current_id(&self) -> RequestId {
        RequestId::new(self.current_id.load(Ordering::Relaxed))
    }

    /// Atomically reserves `count` contiguous ids in a single step, returned
    /// as an inclusive range. `None` for `count == 0` — there is nothing to
    /// reserve. Deliberately a single atomic op, rather than a `reserve_id`
    /// call plus a separate increment composed by the caller, so the
    /// reservation can't race with, or be miscounted against, a concurrent
    /// caller.
    pub fn reserve_range(&self, count: u64) -> Option<RangeInclusive<RequestId>> {
        if count == 0 {
            return None;
        }
        let start = self.current_id.fetch_add(count, Ordering::Relaxed);
        Some(RequestId::new(start)..=RequestId::new(start + count - 1))
    }

    /// Resets the id counter back to zero.
    pub fn reset(&self) {
        self.current_id.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_range_none_for_zero() {
        let idmgr = RequestIdManager::new();
        assert!(idmgr.reserve_range(0).is_none());
        assert_eq!(idmgr.current_id(), RequestId::new(0));
    }

    #[test]
    fn reset_returns_counter_to_zero() {
        let idmgr = RequestIdManager::new();
        idmgr.reserve_range(5).unwrap();
        assert_eq!(idmgr.current_id(), RequestId::new(5));

        idmgr.reset();
        assert_eq!(idmgr.current_id(), RequestId::new(0));
        assert_eq!(idmgr.reserve_id(), RequestId::new(0));
    }

    #[test]
    fn reserve_range_size_matches_count() {
        let idmgr = RequestIdManager::new();
        let range = idmgr.reserve_range(5).unwrap();
        assert_eq!(u64::from(*range.end()) - u64::from(*range.start()) + 1, 5);
    }

    /// Each thread reserves a contiguous range via `reserve_range`. If it
    /// ever raced with a concurrent reservation, the ranges reported here
    /// would overlap or be mis-sized.
    #[test]
    fn reserve_range_is_disjoint_under_concurrency() {
        let idmgr = RequestIdManager::new();
        let batch_size: u64 = 5;
        let batches_per_thread = 200;
        let threads = 8;

        let ranges: Vec<(u64, u64)> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        let mut local = Vec::with_capacity(batches_per_thread);
                        for _ in 0..batches_per_thread {
                            let range = idmgr.reserve_range(batch_size).unwrap();
                            local.push((u64::from(*range.start()), u64::from(*range.end())));
                        }
                        local
                    })
                })
                .collect();

            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect()
        });

        assert_eq!(
            ranges.len() as u64,
            threads as u64 * batches_per_thread as u64
        );
        for &(start, end) in &ranges {
            assert_eq!(
                end - start + 1,
                batch_size,
                "range size must match batch_size"
            );
        }

        let mut sorted = ranges.clone();
        sorted.sort_unstable();
        for window in sorted.windows(2) {
            let (_, prev_end) = window[0];
            let (next_start, _) = window[1];
            assert!(
                next_start > prev_end,
                "ranges must not overlap: {:?} vs {:?}",
                window[0],
                window[1]
            );
        }
    }
}
