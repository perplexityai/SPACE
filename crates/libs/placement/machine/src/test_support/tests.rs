use super::*;

const TABLE: [&str; 20] = [
    "created          A  .",
    "creating         .  .",
    "starting         .  .",
    "started          .  A",
    "restoring        .  .",
    "pausing          .  .",
    "paused           .  .",
    "resuming         .  .",
    "suspending       .  .",
    "suspended        .  !",
    "stopping         .  .",
    "stopped          .  .",
    "deleting         .  .",
    "checkpointing    .  .",
    "quarantined      .  .",
    "failed           .  .",
    "deleted          .  .",
    "cleanup_required A  A",
    "bogus            .  .",
    "_Suspended_      A  .",
];

/// Every stored value's place in the vocabulary, in declaration order.
/// Wildcard-free over both enums, so a new variant does not compile here
/// until it is given an ordinal. That brings whoever adds it to
/// [`STORED_VARIANTS`], but nothing makes them raise it: an appended variant
/// given the next ordinal, with neither the count nor the list updated, still
/// passes. What `the_vocabulary_lists_every_variant_in_declaration_order`
/// does catch is an entry dropped from either list, a list out of
/// declaration order and a live variant declared between two listed ones,
/// which renumbers the later variants.
fn ordinal(stored: StoredSandboxLifecycle) -> usize {
    use SandboxLifecycle as L;
    use StoredSandboxLifecycle as S;
    match stored {
        S::Live(L::Created) => 0,
        S::Live(L::Creating) => 1,
        S::Live(L::Starting) => 2,
        S::Live(L::Started) => 3,
        S::Live(L::Restoring) => 4,
        S::Live(L::Pausing) => 5,
        S::Live(L::Paused) => 6,
        S::Live(L::Resuming) => 7,
        S::Live(L::Suspending) => 8,
        S::Live(L::Suspended) => 9,
        S::Live(L::Stopping) => 10,
        S::Live(L::Stopped) => 11,
        S::Live(L::Deleting) => 12,
        S::Live(L::Checkpointing) => 13,
        S::Live(L::Quarantined) => 14,
        S::Live(L::Failed) => 15,
        S::Deleted => 16,
        S::CleanupRequired => 17,
    }
}

/// The number of arms in [`ordinal`], kept by hand.
const STORED_VARIANTS: usize = 18;

#[test]
fn the_vocabulary_lists_every_variant_in_declaration_order() {
    let all = StoredSandboxLifecycle::all();
    assert_eq!(all.len(), STORED_VARIANTS, "{all:?}");
    let ordinals: Vec<_> = all.iter().map(|stored| ordinal(*stored)).collect();
    assert_eq!(ordinals, (0..STORED_VARIANTS).collect::<Vec<_>>());
    // A variant declared between two others shifts every later
    // discriminant, so `SandboxLifecycle::all` missing it leaves a gap here
    // even before `ordinal` is updated.
    for (index, live) in SandboxLifecycle::all().iter().enumerate() {
        assert_eq!(*live as usize, index, "{live} is out of declaration order");
    }
}

#[test]
fn a_matrix_reads_one_row_per_stored_lifecycle_and_non_canonical_value() {
    let matrix = stored_lifecycle_matrix(["first", "second"], &TABLE);
    let applied: Vec<_> = matrix
        .iter()
        .filter(|(_, cells)| cells.iter().any(|(_, expect)| expect.applies()))
        .map(|&(stored, cells)| (stored, cells.map(|(_, expect)| expect)))
        .collect();
    assert_eq!(
        applied,
        [
            ("created", [Expect::Applies, Expect::Refuses]),
            ("started", [Expect::Refuses, Expect::Applies]),
            ("suspended", [Expect::Refuses, Expect::KnownDefect]),
            ("cleanup_required", [Expect::Applies, Expect::Applies]),
            (" Suspended ", [Expect::Applies, Expect::Refuses]),
        ]
    );
    assert_eq!(known_defects(&matrix), 1);
}

#[test]
#[should_panic(expected = "one row per stored lifecycle")]
fn a_matrix_missing_a_stored_lifecycle_is_refused() {
    stored_lifecycle_matrix(["first", "second"], &TABLE[..19]);
}

#[test]
#[should_panic(expected = "one row per stored lifecycle")]
fn a_matrix_out_of_vocabulary_order_is_refused() {
    let mut table = TABLE;
    table.swap(0, 1);
    stored_lifecycle_matrix(["first", "second"], &table);
}

#[test]
#[should_panic(expected = "row created: 1 marks for 2 writers")]
fn a_row_with_a_missing_cell_is_refused() {
    let mut table = TABLE;
    table[0] = "created          A";
    stored_lifecycle_matrix(["first", "second"], &table);
}
