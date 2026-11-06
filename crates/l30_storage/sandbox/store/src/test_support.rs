//! The stored-lifecycle sweep and matrix oracle shared by the Postgres
//! `sandboxes.lifecycle` writer tests of every crate that writes the row.
//!
//! A writer test seeds each canonical value [`StoredSandboxLifecycle::all`]
//! names, plus the [`NON_CANONICAL_ROWS`] the CHECK-less `text` column can
//! also hold, and checks the writer against a table written out by hand, one
//! row per value. [`stored_lifecycle_matrix`] reads those tables and refuses
//! one that does not list exactly those rows, so a value added to the
//! vocabulary fails every matrix until each table says what its writers do
//! with it.
//!
//! The vocabulary itself is kept by hand: Rust cannot enumerate an enum's
//! variants without a derive. [`SandboxLifecycle::all`] has to list each
//! variant and `TOMBSTONES` each stored-only one. This crate's tests catch a
//! live variant declared between two listed ones (it renumbers the later
//! variants), a list out of declaration order and an entry dropped from
//! either list. A variant appended after the last one, or a tombstone given
//! a position but not listed, still has to be listed by hand.

use pplx_space_common_core::types::SandboxLifecycle;

use crate::StoredSandboxLifecycle;

impl StoredSandboxLifecycle {
    /// Every canonical value the stored vocabulary names, for tests that sweep
    /// a writer over `sandboxes.lifecycle`: each [`SandboxLifecycle::all`]
    /// variant in declaration order, then the tombstones.
    ///
    /// The live half is [`SandboxLifecycle::all`], so a variant listed there
    /// reaches every sweep with no edit here. The tombstone half is
    /// [`TOMBSTONES`]; the wildcard-free match in `tombstone_position` makes a
    /// new stored-only variant fail to compile until it is classified, and
    /// listing it stays manual.
    pub const fn all() -> &'static [Self] {
        &STORED_LIFECYCLES
    }

    /// [`Self::all`] as the strings Postgres stores, for sweeps that seed and
    /// compare raw rows.
    pub fn all_as_str() -> impl Iterator<Item = &'static str> + Clone {
        Self::all().iter().map(|lifecycle| lifecycle.as_str())
    }

    /// The stored values outside the live vocabulary, as the strings Postgres
    /// stores.
    pub fn tombstones_as_str() -> impl Iterator<Item = &'static str> + Clone {
        TOMBSTONES.iter().map(|lifecycle| lifecycle.as_str())
    }
}

/// Live values first, then [`TOMBSTONES`]; see [`StoredSandboxLifecycle::all`].
#[allow(
    clippy::indexing_slicing,
    reason = "evaluated at compile time: an out-of-bounds index fails the build"
)]
const STORED_LIFECYCLES: [StoredSandboxLifecycle;
    SandboxLifecycle::all().len() + TOMBSTONES.len()] = {
    let live = SandboxLifecycle::all();
    let mut stored =
        [StoredSandboxLifecycle::Deleted; SandboxLifecycle::all().len() + TOMBSTONES.len()];
    let mut index = 0;
    while index < live.len() {
        stored[index] = StoredSandboxLifecycle::Live(live[index]);
        index += 1;
    }
    let mut tombstone = 0;
    while tombstone < TOMBSTONES.len() {
        assert!(matches!(
            tombstone_position(TOMBSTONES[tombstone]),
            Some(position) if position == tombstone
        ));
        stored[index + tombstone] = TOMBSTONES[tombstone];
        tombstone += 1;
    }
    stored
};

/// The stored values outside the live vocabulary, in `tombstone_position`
/// order. A new stored-only variant has to be listed here by hand.
const TOMBSTONES: [StoredSandboxLifecycle; 2] = [
    StoredSandboxLifecycle::Deleted,
    StoredSandboxLifecycle::CleanupRequired,
];

/// Each tombstone's position in [`TOMBSTONES`], which the build of
/// [`STORED_LIFECYCLES`] checks for every listed tombstone. Wildcard-free over
/// [`StoredSandboxLifecycle`], so a new stored-only variant fails to compile
/// here until it is given a position; list it at that position in
/// [`TOMBSTONES`].
const fn tombstone_position(lifecycle: StoredSandboxLifecycle) -> Option<usize> {
    match lifecycle {
        StoredSandboxLifecycle::Live(_) => None,
        StoredSandboxLifecycle::Deleted => Some(0),
        StoredSandboxLifecycle::CleanupRequired => Some(1),
    }
}

/// Values outside the canonical vocabulary that the CHECK-less `text` column
/// can still hold, swept after it as `(table label, stored value)`. Writers
/// differ on them — some compare `LOWER(TRIM(lifecycle))`, most compare the
/// value exactly — so each matrix pins what every writer does today. The
/// padded, capitalized spelling is of a live value a normalizing guard
/// admits.
pub const NON_CANONICAL_ROWS: [(&str, &str); 2] =
    [("bogus", "bogus"), ("_Suspended_", " Suspended ")];

/// One cell of a writer × stored-lifecycle expectation table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expect {
    /// `A`: the writer applies from this stored value.
    Applies,
    /// `!`: the writer applies today, and that is a known defect the test's
    /// doc names. Asserted exactly like [`Expect::Applies`]; fixing the
    /// defect means changing the mark to `.`.
    KnownDefect,
    /// `.`: the writer refuses and leaves the row as it was.
    Refuses,
}

impl Expect {
    /// Whether the writer moves the row from this stored value.
    pub fn applies(self) -> bool {
        match self {
            Self::Applies | Self::KnownDefect => true,
            Self::Refuses => false,
        }
    }
}

/// Reads a writer × stored-lifecycle expectation table.
///
/// `table` has one row per [`StoredSandboxLifecycle::all`] entry, in that
/// order, then one per [`NON_CANONICAL_ROWS`] label. A row is the label
/// followed by `N` whitespace-separated marks, one per entry of `writers`:
/// `A` where the writer applies, `!` where it applies as a known defect and
/// `.` where it refuses. Returns, per row, the value to store and each writer
/// with its expectation; `writers` and each row's marks share `N`, so a
/// column cannot be dropped.
///
/// # Panics
///
/// When the rows do not name exactly that vocabulary in order, or a row has
/// other than `N` marks or a mark other than `A`, `!` or `.`.
pub fn stored_lifecycle_matrix<W: Copy, const N: usize>(
    writers: [W; N],
    table: &[&'static str],
) -> Vec<(&'static str, [(W, Expect); N])> {
    let rows: Vec<(&'static str, Vec<&'static str>)> = table
        .iter()
        .map(|row| {
            let mut cells = row.split_whitespace();
            let label = cells.next().unwrap_or_default();
            (label, cells.collect())
        })
        .collect();
    let listed: Vec<_> = rows.iter().map(|(label, _)| *label).collect();
    let vocabulary: Vec<_> = StoredSandboxLifecycle::all_as_str()
        .chain(NON_CANONICAL_ROWS.iter().map(|(label, _)| *label))
        .collect();
    assert_eq!(
        listed, vocabulary,
        "a lifecycle matrix needs one row per stored lifecycle, in \
         `StoredSandboxLifecycle::all` order, then the `NON_CANONICAL_ROWS`"
    );
    rows.into_iter()
        .map(|(label, marks)| {
            assert_eq!(
                marks.len(),
                N,
                "row {label}: {} marks for {N} writers",
                marks.len()
            );
            let stored = NON_CANONICAL_ROWS
                .iter()
                .find(|(non_canonical, _)| *non_canonical == label)
                .map_or(label, |(_, value)| *value);
            let mut cells = writers.map(|writer| (writer, Expect::Refuses));
            for ((_, cell), mark) in cells.iter_mut().zip(marks) {
                assert!(
                    matches!(mark, "A" | "!" | "."),
                    "row {label}: unknown mark {mark:?}; use `A`, `!` or `.`"
                );
                *cell = match mark {
                    "A" => Expect::Applies,
                    "!" => Expect::KnownDefect,
                    _ => Expect::Refuses,
                };
            }
            (stored, cells)
        })
        .collect()
}

/// The number of [`Expect::KnownDefect`] cells in a matrix, which a test with
/// known defects pins so that fixing one is a visible change to its table.
pub fn known_defects<W, const N: usize>(matrix: &[(&str, [(W, Expect); N])]) -> usize {
    matrix
        .iter()
        .flat_map(|(_, cells)| cells.iter())
        .filter(|(_, expect)| *expect == Expect::KnownDefect)
        .count()
}

#[cfg(test)]
mod tests;
