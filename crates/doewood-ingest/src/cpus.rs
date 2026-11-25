//! The instance's CPU slots, shared by every analysis (issue #101).
//!
//! Each running analysis holds one slot ([`CpuSlots::enter`]) for as long as it runs, an
//! overrun's included. A spreadsheet's normalizer may borrow the slots nobody holds
//! ([`CpuSlots::lend`]) for the length of its call, up to a cap, and gives them back when the
//! call returns, panics or unwinds ([`Lent`]'s drop). So one big workbook on an idle instance
//! gets several hedgehog threads, and under load each analysis keeps one.
//!
//! The rule: a loan only takes slots that are free at that moment, so lent slots never take
//! the count over the CPUs. An analysis always gets its own slot, even when every slot is in
//! use (it never waits for one): an analysis that starts while threads are lent runs beside
//! them until that normalize returns, and the next normalize borrows nothing.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// The most threads one spreadsheet's normalizer runs with, lent slots included. hedgehog's
/// row model streams on at most 4 threads; each thread adds a little working memory (see
/// docs/ingest.md, Memory).
pub const MAX_SHEET_THREADS: usize = 4;

/// The CPUs this process may use: `available_parallelism`, which follows a cgroup CPU quota
/// and the affinity mask.
pub fn available() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}

/// Slots in use: one per running analysis, plus the slots lent to normalizers.
#[derive(Debug)]
pub struct CpuSlots {
    total: usize,
    used: AtomicUsize,
}

impl CpuSlots {
    /// `total` slots (at least 1).
    pub fn new(total: usize) -> Arc<Self> {
        Arc::new(Self {
            total: total.max(1),
            used: AtomicUsize::new(0),
        })
    }

    /// The slots.
    pub fn total(&self) -> usize {
        self.total
    }

    /// Slots in use now.
    pub fn in_use(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    /// One analysis's own slot, held until the guard drops. Never waits.
    pub fn enter(self: &Arc<Self>) -> Slot {
        self.used.fetch_add(1, Ordering::SeqCst);
        Slot {
            of: Arc::clone(self),
        }
    }

    /// Lends a normalizer the free slots, at most `max_extra`; none when no slot is free. The
    /// caller runs with [`Lent::threads`] threads: its own slot plus the loan.
    pub fn lend(self: &Arc<Self>, max_extra: usize) -> Lent {
        let mut used = self.used.load(Ordering::SeqCst);
        let extra = loop {
            let extra = self.total.saturating_sub(used).min(max_extra);
            if extra == 0 {
                break 0;
            }
            match self.used.compare_exchange_weak(
                used,
                used + extra,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break extra,
                Err(now) => used = now,
            }
        };
        Lent {
            of: Arc::clone(self),
            extra,
        }
    }
}

/// An analysis's own slot ([`CpuSlots::enter`]).
#[derive(Debug)]
pub struct Slot {
    of: Arc<CpuSlots>,
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.of.used.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Slots lent to one normalizer ([`CpuSlots::lend`]); given back on drop.
#[derive(Debug)]
pub struct Lent {
    of: Arc<CpuSlots>,
    extra: usize,
}

impl Lent {
    /// The threads to run with: the caller's own one plus the lent slots.
    pub fn threads(&self) -> usize {
        1 + self.extra
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        if self.extra > 0 {
            self.of.used.fetch_sub(self.extra, Ordering::SeqCst);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_idle_instance_lends_up_to_the_cap() {
        let cpus = CpuSlots::new(4);
        let _own = cpus.enter();
        let lent = cpus.lend(MAX_SHEET_THREADS - 1);
        assert_eq!(lent.threads(), 4);
        assert_eq!(cpus.in_use(), 4);
        // Nothing is left for a second normalizer.
        let _other = cpus.enter();
        assert_eq!(cpus.lend(3).threads(), 1);
        drop(lent);
        assert_eq!(cpus.in_use(), 2);
        assert_eq!(cpus.lend(1).threads(), 2);
        assert_eq!(cpus.in_use(), 2);
    }

    #[test]
    fn a_busy_instance_lends_nothing() {
        let cpus = CpuSlots::new(2);
        let (_a, _b) = (cpus.enter(), cpus.enter());
        assert_eq!(cpus.lend(3).threads(), 1);
        assert_eq!(cpus.in_use(), 2);
        assert_eq!(CpuSlots::new(0).total(), 1);
    }

    /// Slots come back when a normalizer panics: the guards drop on unwind.
    #[test]
    fn a_panic_gives_the_slots_back() {
        let cpus = CpuSlots::new(4);
        let c = Arc::clone(&cpus);
        let r = std::panic::catch_unwind(move || {
            let _own = c.enter();
            let lent = c.lend(3);
            assert_eq!(c.in_use(), 4);
            assert_eq!(lent.threads(), 4);
            panic!("normalizer panicked");
        });
        assert!(r.is_err());
        assert_eq!(cpus.in_use(), 0);
    }

    /// Loans racing on threads of their own, with every worker's slot already held: the
    /// count never goes over the CPUs, so a loan only ever takes free slots.
    #[test]
    fn concurrent_loans_never_exceed_the_cpus() {
        for (total, workers) in [(4, 1), (4, 2), (4, 4), (2, 2), (8, 3), (8, 8)] {
            let cpus = CpuSlots::new(total);
            let start = Arc::new(std::sync::Barrier::new(workers));
            let peak = Arc::new(AtomicUsize::new(0));
            let handles: Vec<_> = (0..workers)
                .map(|_| {
                    let (cpus, start, peak) =
                        (Arc::clone(&cpus), Arc::clone(&start), Arc::clone(&peak));
                    std::thread::spawn(move || {
                        let own = cpus.enter();
                        start.wait();
                        let mut lent_some = false;
                        for i in 0..20_000usize {
                            let lent = cpus.lend(MAX_SHEET_THREADS - 1);
                            lent_some |= lent.threads() > 1;
                            peak.fetch_max(cpus.in_use(), Ordering::SeqCst);
                            assert!(lent.threads() <= MAX_SHEET_THREADS);
                            if i % 7 == 0 {
                                std::thread::yield_now();
                            }
                            drop(lent);
                        }
                        // Every worker still holds its slot until all loops are done.
                        start.wait();
                        drop(own);
                        lent_some
                    })
                })
                .collect();
            // Join them all (`any` alone would stop at the first that lent).
            let lent: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let lent_some = lent.contains(&true);
            let peak = peak.load(Ordering::SeqCst);
            assert!(
                peak <= total,
                "{total} CPUs, {workers} workers: peak {peak}"
            );
            assert_eq!(
                lent_some,
                workers < total,
                "{total} CPUs, {workers} workers"
            );
            assert_eq!(cpus.in_use(), 0);
        }
    }

    /// Workers that enter, borrow and return in a loop: all slots come back. A worker that
    /// enters while slots are lent never waits, so the count may pass the CPUs by at most one
    /// slot per such worker, and only until the loan is returned.
    #[test]
    fn concurrent_workers_give_every_slot_back() {
        let (total, workers) = (4, 4);
        let cpus = CpuSlots::new(total);
        let peak = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..workers)
            .map(|_| {
                let (cpus, peak) = (Arc::clone(&cpus), Arc::clone(&peak));
                std::thread::spawn(move || {
                    for _ in 0..20_000 {
                        let own = cpus.enter();
                        let lent = cpus.lend(MAX_SHEET_THREADS - 1);
                        peak.fetch_max(cpus.in_use(), Ordering::SeqCst);
                        drop(lent);
                        drop(own);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let peak = peak.load(Ordering::SeqCst);
        assert!(peak < total + workers, "peak {peak}");
        assert_eq!(cpus.in_use(), 0);
    }
}
