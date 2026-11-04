//! Accumulated elapsed time on the monotonic clock.
//!
//! [`Stopwatch`] measures elapsed time with [`Instant`], which never goes
//! backwards. It never uses `SystemTime`, which clock corrections can move.
//! Like Guava's `Stopwatch`, it accumulates across separate runs and reports a
//! typed [`Duration`] rather than a unitless number.
//!
//! A run is either a scope or a closure, so it can't be left running or stopped
//! twice. [`Stopwatch::start`] returns a [`Run`] guard that adds its elapsed
//! time when dropped, including on early return, `?`, or unwind.
//! [`Stopwatch::time`] times one closure, and [`Stopwatch::time_each`] times
//! each `next()` of an iterator, such as blocking channel receives.
//!
//! These are elapsed times, not CPU time: preemption, I/O, and blocking all
//! count. Each run reads the clock twice, so time runs around coarse units of
//! work, not tight inner loops.
//!
//! ```rust
//! use pplx_space_stopwatch::Stopwatch;
//!
//! let mut busy = Stopwatch::new();
//! let mut wait = Stopwatch::new();
//! let mut sum = 0u64;
//! for value in wait.time_each([1u64, 2, 3]) {
//!     sum += busy.time(|| value * 2);
//! }
//! assert_eq!(sum, 12);
//! assert!(busy.elapsed() + wait.elapsed() >= busy.elapsed());
//! ```

use std::time::{Duration, Instant};

/// Elapsed time accumulated over any number of runs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Stopwatch {
    elapsed: Duration,
}

impl Stopwatch {
    /// A stopwatch with no recorded time.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            elapsed: Duration::ZERO,
        }
    }

    /// Total time recorded by finished runs.
    #[must_use]
    pub const fn elapsed(&self) -> Duration {
        self.elapsed
    }

    /// Start a run that records its elapsed time when the guard drops.
    pub fn start(&mut self) -> Run<'_> {
        Run {
            stopwatch: self,
            started: Instant::now(),
        }
    }

    /// Run `operation`, record how long it took, and return its result.
    pub fn time<T>(&mut self, operation: impl FnOnce() -> T) -> T {
        let _run = self.start();
        operation()
    }

    /// Time each `next()` call of `iter`, including the final `None`.
    pub fn time_each<I: IntoIterator>(&mut self, iter: I) -> TimeEach<'_, I::IntoIter> {
        TimeEach {
            stopwatch: self,
            iter: iter.into_iter(),
        }
    }

    /// Record time measured elsewhere, saturating at [`Duration::MAX`].
    pub fn add(&mut self, elapsed: Duration) {
        self.elapsed = self.elapsed.saturating_add(elapsed);
    }
}

/// Run `operation` and return its result with the elapsed time.
pub fn measure<T>(operation: impl FnOnce() -> T) -> (T, Duration) {
    let started = Instant::now();
    let result = operation();
    (result, started.elapsed())
}

/// An in-progress [`Stopwatch`] run, recorded when dropped.
#[must_use = "a run records its time when dropped; an unbound guard drops immediately"]
#[derive(Debug)]
pub struct Run<'a> {
    stopwatch: &'a mut Stopwatch,
    started: Instant,
}

impl Run<'_> {
    /// Time elapsed in this run so far.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }
}

impl Drop for Run<'_> {
    fn drop(&mut self) {
        self.stopwatch.add(self.started.elapsed());
    }
}

/// Iterator returned by [`Stopwatch::time_each`].
#[derive(Debug)]
pub struct TimeEach<'a, I> {
    stopwatch: &'a mut Stopwatch,
    iter: I,
}

impl<I: Iterator> Iterator for TimeEach<'_, I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let Self { stopwatch, iter } = self;
        stopwatch.time(|| iter.next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

#[cfg(test)]
mod tests;
