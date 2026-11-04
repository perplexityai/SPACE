//! The metric emit core shared by SPACE's metric definitions: the
//! `counter`, `gauge`, `timing_ms` and `distribution` wrappers over
//! `pplx-space-observability`, the `tag` and `elapsed_ms` helpers, the
//! thread-local test capture behind them, and the `metric_vocabulary!` macro
//! for bounded tag values.
//!
//! The emit functions are shared plumbing for the metric modules: other
//! crates emit through a metrics module's named functions, not by calling
//! these directly.
//!
//! It is its own crate so that a metric module can emit through it without
//! depending on `pplx-space-metrics` and, through it, on
//! `pplx-space-common-core`. `pplx-space-metrics` re-exports it from its
//! private `emit` module and its public `test_capture` module.

/// Declare a bounded metric-tag vocabulary: the enum, its canonical `as_str`,
/// and a `Display` that agrees with it.
///
/// Every tag value SPACE emits has to come from a closed set — an unbounded
/// tag is a cardinality incident — and the three pieces have to agree, so they
/// are generated together rather than written three times per enum.
///
/// Doc comments are forwarded, on the enum and on each variant, because what a
/// tag value *means* is the part an operator reading the metrics catalog needs
/// and the part a hand-written copy tends to omit.
///
/// Exported from the crate root; `pplx-space-metrics` imports it with
/// `#[macro_use]`, so every module there can invoke it without a `use`.
#[macro_export]
macro_rules! metric_vocabulary {
    (
        $(#[$enum_meta:meta])*
        $name:ident { $($(#[$variant_meta:meta])* $variant:ident => $value:literal),+ $(,)? }
    ) => {
        $(#[$enum_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

use std::time::Instant;

pub fn elapsed_ms(started_at: Instant) -> f64 {
    pplx_space_observability::elapsed_ms(started_at)
}

pub fn tag(key: &str, value: impl ToString) -> String {
    pplx_space_observability::tag(key, value)
}

pub fn counter(name: &str, value: u64, tags: &[String]) {
    #[cfg(any(test, feature = "test-capture"))]
    test_capture::record(name, value, tags);
    pplx_space_observability::counter(name, value, tags);
}

/// Thread-local counter interception for unit tests. A global sink can't back
/// per-test assertions here: observability's `METRICS` is a set-once
/// `OnceLock` with a crate-private sink trait, and cargo runs unit tests as
/// parallel threads in one process. `placement_directed` (and every `counter`
/// caller) runs synchronously on the asserting thread, so a thread-local is
/// both correct and isolated.
#[cfg(any(test, feature = "test-capture"))]
pub mod test_capture {
    use std::{
        cell::{Cell, RefCell},
        marker::PhantomData,
        rc::Rc,
        thread::LocalKey,
    };

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct CapturedCounter {
        pub name: String,
        pub value: u64,
        pub tags: Vec<String>,
    }

    #[derive(Clone, Debug, PartialEq)]
    pub struct CapturedValue {
        pub name: String,
        pub value: f64,
        pub tags: Vec<String>,
    }

    struct Capture<T> {
        generation: u64,
        events: Vec<T>,
    }

    thread_local! {
        static NEXT_GENERATION: Cell<u64> = const { Cell::new(1) };
        static BUFFER: RefCell<Vec<Capture<CapturedCounter>>> = const { RefCell::new(Vec::new()) };
        static VALUE_BUFFER: RefCell<Vec<Capture<CapturedValue>>> = const { RefCell::new(Vec::new()) };
    }

    fn install<T: 'static>(buffer: &'static LocalKey<RefCell<Vec<Capture<T>>>>) -> u64 {
        let generation = NEXT_GENERATION.with(|next| {
            let generation = next.get();
            next.set(
                generation
                    .checked_add(1)
                    .expect("metrics test-capture generation overflow"),
            );
            generation
        });
        buffer.with(|buffer| {
            buffer.borrow_mut().push(Capture {
                generation,
                events: Vec::new(),
            });
        });
        generation
    }

    fn finish<T: 'static>(
        buffer: &'static LocalKey<RefCell<Vec<Capture<T>>>>,
        generation: u64,
    ) -> Vec<T> {
        buffer.with(|buffer| {
            let mut capture = buffer.borrow_mut();
            let Some(index) = capture
                .iter()
                .position(|capture| capture.generation == generation)
            else {
                return Vec::new();
            };
            capture.remove(index).events
        })
    }

    fn clear<T: 'static>(buffer: &'static LocalKey<RefCell<Vec<Capture<T>>>>, generation: u64) {
        buffer.with(|buffer| {
            let mut capture = buffer.borrow_mut();
            if let Some(index) = capture
                .iter()
                .position(|capture| capture.generation == generation)
            {
                capture.remove(index);
            }
        });
    }

    pub(crate) fn record(name: &str, value: u64, tags: &[String]) {
        BUFFER.with(|b| {
            if let Some(capture) = b.borrow_mut().last_mut() {
                capture.events.push(CapturedCounter {
                    name: name.to_string(),
                    value,
                    tags: tags.to_vec(),
                });
            }
        });
    }

    /// Capture counters emitted on this thread until the returned guard is
    /// finished or dropped. Starting another counter capture temporarily
    /// suspends this one; finishing or dropping the nested guard resumes it.
    pub fn capture_counters_guard() -> CounterCaptureGuard {
        CounterCaptureGuard {
            generation: Some(install(&BUFFER)),
            _not_send: PhantomData,
        }
    }

    pub struct CounterCaptureGuard {
        generation: Option<u64>,
        // Capture storage is thread-local, so the guard must not move between
        // threads while it is held across an await point.
        _not_send: PhantomData<Rc<()>>,
    }

    impl CounterCaptureGuard {
        pub fn finish(mut self) -> Vec<CapturedCounter> {
            self.generation
                .take()
                .map(|generation| finish(&BUFFER, generation))
                .unwrap_or_default()
        }
    }

    impl Drop for CounterCaptureGuard {
        fn drop(&mut self) {
            if let Some(generation) = self.generation.take() {
                clear(&BUFFER, generation);
            }
        }
    }

    pub(crate) fn record_value(name: &str, value: f64, tags: &[String]) {
        VALUE_BUFFER.with(|buffer| {
            if let Some(capture) = buffer.borrow_mut().last_mut() {
                capture.events.push(CapturedValue {
                    name: name.to_string(),
                    value,
                    tags: tags.to_vec(),
                });
            }
        });
    }

    pub fn capture_values(f: impl FnOnce()) -> Vec<CapturedValue> {
        let capture = capture_values_guard();
        f();
        capture.finish()
    }

    /// Capture gauges, timings, and distributions emitted on this thread
    /// until the returned guard is finished or dropped. Nesting semantics
    /// match [`capture_counters_guard`].
    pub fn capture_values_guard() -> ValueCaptureGuard {
        ValueCaptureGuard {
            generation: Some(install(&VALUE_BUFFER)),
            _not_send: PhantomData,
        }
    }

    pub struct ValueCaptureGuard {
        generation: Option<u64>,
        // Capture storage is thread-local, so the guard must not move between
        // threads while it is held across an await point.
        _not_send: PhantomData<Rc<()>>,
    }

    impl ValueCaptureGuard {
        pub fn finish(mut self) -> Vec<CapturedValue> {
            self.generation
                .take()
                .map(|generation| finish(&VALUE_BUFFER, generation))
                .unwrap_or_default()
        }
    }

    impl Drop for ValueCaptureGuard {
        fn drop(&mut self) {
            if let Some(generation) = self.generation.take() {
                clear(&VALUE_BUFFER, generation);
            }
        }
    }

    #[cfg(test)]
    mod tests;
}

pub fn gauge(name: &str, value: f64, tags: &[String]) {
    #[cfg(any(test, feature = "test-capture"))]
    test_capture::record_value(name, value, tags);
    pplx_space_observability::gauge(name, value, tags);
}

pub fn timing_ms(name: &str, value_ms: f64, tags: &[String]) {
    #[cfg(any(test, feature = "test-capture"))]
    test_capture::record_value(name, value_ms, tags);
    pplx_space_observability::timing_ms(name, value_ms, tags);
}

pub fn distribution(name: &str, value: f64, tags: &[String]) {
    #[cfg(any(test, feature = "test-capture"))]
    test_capture::record_value(name, value, tags);
    pplx_space_observability::distribution(name, value, tags);
}
