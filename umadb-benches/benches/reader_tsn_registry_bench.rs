//! Micro-benchmark for the reader-TSN registry in `Mvcc`.
//!
//! Background: `Mvcc` tracks the snapshot (TSN) held by every live reader so the
//! writer can compute the smallest live TSN and decide which freed pages are safe
//! to reuse. The registry is exercised by exactly three operations:
//!
//!   * `register`   — `Mvcc::reader()`, once per read batch (high churn under load)
//!   * `unregister` — `Reader::drop`
//!   * `min`        — `Writer::find_reusable_page_ids`, once per commit
//!
//! The current implementation is a `DashMap<reader_id, Tsn>` and computes `min`
//! with a full `O(live readers)` scan (`reader_tsns.iter().map(..).min()`).
//! This bench isolates those three operations so we can compare the current
//! structure against alternatives that offer `O(1)` (really `O(log n)`) `min`.
//!
//! Two axes matter and are both measured:
//!   * scaling of `min` with the number of live readers `N` (uncontended), and
//!   * latency of `min` under concurrent `register`/`unregister` churn — because
//!     the trade-off is real: `DashMap` gives sharded (concurrent) churn but an
//!     `O(N)` scan; a `Mutex<BTreeMap>` multiset gives `O(1)` min but serializes
//!     every operation behind one lock.
//!
//! Run with, e.g.:
//!   cargo bench -p umadb-benches --bench reader_tsn_registry_bench

use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main};
use dashmap::DashMap;

/// The three registry operations exercised by `Mvcc`, behind a common interface
/// so candidate data structures can be compared apples-to-apples.
trait TsnRegistry: Send + Sync + 'static {
    fn new() -> Self
    where
        Self: Sized;
    /// Register a live reader holding snapshot `tsn` (called per read batch).
    fn register(&self, reader_id: usize, tsn: u64);
    /// Drop a live reader (called on `Reader::drop`).
    fn unregister(&self, reader_id: usize, tsn: u64);
    /// Smallest live TSN, or `None` if there are no live readers (called per commit).
    fn min(&self) -> Option<u64>;
    fn name() -> &'static str
    where
        Self: Sized;
}

/// Candidate 0: the current implementation — `DashMap` keyed by reader id, with an
/// `O(N)` scan for `min`. Churn is sharded/concurrent; `min` walks every entry.
struct DashMapScan {
    map: DashMap<usize, u64>,
}

impl TsnRegistry for DashMapScan {
    fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }
    fn register(&self, reader_id: usize, tsn: u64) {
        self.map.insert(reader_id, tsn);
    }
    fn unregister(&self, reader_id: usize, _tsn: u64) {
        self.map.remove(&reader_id);
    }
    fn min(&self) -> Option<u64> {
        self.map.iter().map(|r| *r.value()).min()
    }
    fn name() -> &'static str {
        "dashmap_scan(current)"
    }
}

/// Counting multiset of live TSNs keyed by TSN. `min` is the first key (`O(1)`),
/// `register`/`unregister` are `O(log n)` — all behind a single lock, so churn is
/// serialized. `reader_id` is unused (the multiset is keyed by TSN).
///
/// Generic over the mutex kind so we can compare `std` vs `parking_lot` locks.
struct BTreeMultiset<L: MutexLike<BTreeMap<u64, usize>>> {
    map: L,
}

impl<L: MutexLike<BTreeMap<u64, usize>> + 'static> TsnRegistry for BTreeMultiset<L> {
    fn new() -> Self {
        Self {
            map: L::new(BTreeMap::new()),
        }
    }
    fn register(&self, _reader_id: usize, tsn: u64) {
        self.map.with(|m| {
            *m.entry(tsn).or_insert(0) += 1;
        });
    }
    fn unregister(&self, _reader_id: usize, tsn: u64) {
        self.map.with(|m| {
            if let Some(count) = m.get_mut(&tsn) {
                *count -= 1;
                if *count == 0 {
                    m.remove(&tsn);
                }
            }
        });
    }
    fn min(&self) -> Option<u64> {
        self.map.with(|m| m.keys().next().copied())
    }
    fn name() -> &'static str {
        L::LABEL
    }
}

/// Minimal abstraction over a mutex so `BTreeMultiset` can be instantiated with
/// either the std or `parking_lot` lock.
trait MutexLike<T>: Send + Sync {
    const LABEL: &'static str;
    fn new(value: T) -> Self;
    fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T: Send> MutexLike<T> for Mutex<T> {
    const LABEL: &'static str = "std_mutex_btree_multiset";
    fn new(value: T) -> Self {
        Mutex::new(value)
    }
    fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.lock().unwrap())
    }
}

impl<T: Send> MutexLike<T> for parking_lot::Mutex<T> {
    const LABEL: &'static str = "parking_lot_mutex_btree_multiset";
    fn new(value: T) -> Self {
        parking_lot::Mutex::new(value)
    }
    fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        f(&mut self.lock())
    }
}

type StdMultiset = BTreeMultiset<Mutex<BTreeMap<u64, usize>>>;
type PlMultiset = BTreeMultiset<parking_lot::Mutex<BTreeMap<u64, usize>>>;

/// Pre-fill a registry with `n` distinct live readers (worst case for the multiset:
/// `n` distinct keys). Returns the next free reader id.
fn prefill<R: TsnRegistry>(reg: &R, n: usize) -> usize {
    for i in 0..n {
        reg.register(i, i as u64);
    }
    n
}

/// Group A: `min` cost vs number of live readers `N`, no contention.
/// This is the decisive `O(N)` (scan) vs `O(1)` (multiset) comparison.
fn bench_min_uncontended<R: TsnRegistry>(group: &mut BenchmarkGroup<WallTime>, sizes: &[usize]) {
    for &n in sizes {
        let reg = R::new();
        prefill(&reg, n);
        group.bench_with_input(BenchmarkId::new(R::name(), n), &n, |b, _| {
            b.iter(|| black_box(reg.min()));
        });
    }
}

/// Group B: cost of one `register` + `unregister` pair (reader churn) vs `N`.
fn bench_churn_uncontended<R: TsnRegistry>(group: &mut BenchmarkGroup<WallTime>, sizes: &[usize]) {
    for &n in sizes {
        let reg = R::new();
        let next_id = AtomicUsize::new(prefill(&reg, n));
        group.bench_with_input(BenchmarkId::new(R::name(), n), &n, |b, _| {
            b.iter(|| {
                // A transient reader arrives and leaves, as happens each read batch.
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                let tsn = (id % 4096) as u64;
                reg.register(id, tsn);
                reg.unregister(id, tsn);
            });
        });
    }
}

/// Group C: latency of the writer's `min` while `churn_threads` background threads
/// continuously register/unregister transient readers (fixed base of `base_n`
/// long-lived readers). Surfaces the lock-contention trade-off.
fn bench_min_under_churn<R: TsnRegistry>(
    group: &mut BenchmarkGroup<WallTime>,
    base_n: usize,
    churn_threads: &[usize],
) {
    for &k in churn_threads {
        let reg = Arc::new(R::new());
        prefill(reg.as_ref(), base_n);

        let stop = Arc::new(AtomicBool::new(false));
        let next_id = Arc::new(AtomicUsize::new(base_n));
        let mut handles = Vec::with_capacity(k);
        for _ in 0..k {
            let reg = reg.clone();
            let stop = stop.clone();
            let next_id = next_id.clone();
            handles.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let id = next_id.fetch_add(1, Ordering::Relaxed);
                    let tsn = (id % 4096) as u64;
                    reg.register(id, tsn);
                    reg.unregister(id, tsn);
                }
            }));
        }

        group.bench_with_input(
            BenchmarkId::new(R::name(), format!("base{base_n}/churn{k}")),
            &k,
            |b, _| {
                b.iter(|| black_box(reg.min()));
            },
        );

        stop.store(true, Ordering::Relaxed);
        for h in handles {
            let _ = h.join();
        }
    }
}

fn reader_tsn_registry_benchmark(c: &mut Criterion) {
    let min_sizes = [0usize, 16, 64, 256, 1024, 4096, 16384];
    let churn_sizes = [0usize, 64, 1024, 16384];
    let churn_threads = [0usize, 1, 2, 4, 8, 16];
    const BASE_N: usize = 1024;

    {
        let mut group = c.benchmark_group("reader_tsn_min_uncontended");
        bench_min_uncontended::<DashMapScan>(&mut group, &min_sizes);
        bench_min_uncontended::<StdMultiset>(&mut group, &min_sizes);
        bench_min_uncontended::<PlMultiset>(&mut group, &min_sizes);
        group.finish();
    }

    {
        let mut group = c.benchmark_group("reader_tsn_churn_uncontended");
        bench_churn_uncontended::<DashMapScan>(&mut group, &churn_sizes);
        bench_churn_uncontended::<StdMultiset>(&mut group, &churn_sizes);
        bench_churn_uncontended::<PlMultiset>(&mut group, &churn_sizes);
        group.finish();
    }

    {
        let mut group = c.benchmark_group("reader_tsn_min_under_churn");
        bench_min_under_churn::<DashMapScan>(&mut group, BASE_N, &churn_threads);
        bench_min_under_churn::<StdMultiset>(&mut group, BASE_N, &churn_threads);
        bench_min_under_churn::<PlMultiset>(&mut group, BASE_N, &churn_threads);
        group.finish();
    }
}

criterion_group!(benches, reader_tsn_registry_benchmark);
criterion_main!(benches);
