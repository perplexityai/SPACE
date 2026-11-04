//! An opt-in record of this process's own WARN and ERROR callsites.
//!
//! A process whose logs no collector reads (the guest's spaced) enables the
//! tap and hands what it recorded to a host that does. The record holds only
//! callsite identity — level, target, source file and line — and how often
//! each fired. Message text and field values are never kept: a formatted
//! message can carry a user's command, path or output.
//!
//! The record is bounded: at most [`MAX_CALLSITES`] distinct callsites are kept
//! between drains, and events at further callsites are only counted.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Layer};

/// Distinct callsites kept between drains.
pub const MAX_CALLSITES: usize = 32;

static ENABLED: AtomicBool = AtomicBool::new(false);
static RECORD: Mutex<Record> = Mutex::new(Record {
    callsites: Vec::new(),
    dropped: 0,
});

struct Record {
    callsites: Vec<(&'static Metadata<'static>, u64)>,
    dropped: u64,
}

/// One WARN or ERROR callsite and how often it fired since the last drain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WarningCallsite {
    pub level: WarningLevel,
    pub target: &'static str,
    pub file: &'static str,
    pub line: u32,
    pub count: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningLevel {
    Warn,
    Error,
}

impl WarningLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warn => "warn",
            Self::Error => "error",
        }
    }
}

/// What a drain returns: the recorded callsites, and how many events fired at
/// callsites past the bound.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WarningDrain {
    pub callsites: Vec<WarningCallsite>,
    pub dropped: u64,
}

/// Start recording. Until a process calls this, the tap ignores every event.
pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Take everything recorded since the previous drain.
pub fn drain() -> WarningDrain {
    let mut record = RECORD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let callsites = std::mem::take(&mut record.callsites)
        .into_iter()
        .map(|(metadata, count)| WarningCallsite {
            level: if *metadata.level() == Level::ERROR {
                WarningLevel::Error
            } else {
                WarningLevel::Warn
            },
            target: metadata.target(),
            file: metadata.file().unwrap_or(""),
            line: metadata.line().unwrap_or(0),
            count,
        })
        .collect();
    WarningDrain {
        callsites,
        dropped: std::mem::take(&mut record.dropped),
    }
}

/// Forget everything recorded. A guest restored from a snapshot (a fork or a
/// template's base, say) holds its parent's record in memory, and those
/// callsites fired in another sandbox.
pub fn discard() {
    drop(drain());
}

/// The tap's layer, for a test's own subscriber; the shared subscriber
/// installs it itself.
#[cfg(any(test, feature = "test-capture"))]
pub fn layer_for_test<S: Subscriber>() -> impl Layer<S> {
    WarningTapLayer
}

/// The layer the shared subscriber installs, behind a per-layer WARN filter
/// so it never changes what any other layer sees.
pub(crate) struct WarningTapLayer;

impl<S: Subscriber> Layer<S> for WarningTapLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Events the log formatter drops, such as library notices, are not
        // diagnostics worth handing over either.
        let metadata = event.metadata();
        if !ENABLED.load(Ordering::Relaxed)
            || *metadata.level() > Level::WARN
            || !crate::should_format_event(metadata)
        {
            return;
        }
        record(metadata);
    }
}

fn record(metadata: &'static Metadata<'static>) {
    let mut record = RECORD
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some((_, count)) = record
        .callsites
        .iter_mut()
        .find(|(known, _)| std::ptr::eq(*known, metadata))
    {
        *count = count.saturating_add(1);
    } else if record.callsites.len() < MAX_CALLSITES {
        record.callsites.push((metadata, 1));
    } else {
        record.dropped = record.dropped.saturating_add(1);
    }
}
