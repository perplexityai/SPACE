use super::*;

const GENERATION: i64 = 5;

fn row(
    generation: i64,
    active: bool,
    replaced: bool,
    materialized: bool,
) -> ExecutionSupersessionRow {
    ExecutionSupersessionRow::new(generation, active, replaced, materialized)
}

#[test]
fn no_rows_is_not_superseded() {
    assert!(!execution_generation_superseded(&[], GENERATION));
}

#[test]
fn own_execution_retired_as_replaced_is_superseded() {
    assert!(execution_generation_superseded(
        &[row(GENERATION, false, true, true)],
        GENERATION
    ));
}

#[test]
fn own_execution_retired_otherwise_is_not_superseded() {
    // A suspend the sweeper aged out, an activation rollback, a pod found
    // missing: nothing newer, so the runtime may hold the latest state.
    assert!(!execution_generation_superseded(
        &[row(GENERATION, false, false, true)],
        GENERATION
    ));
}

#[test]
fn own_execution_active_is_not_superseded_even_if_marked_replaced() {
    assert!(!execution_generation_superseded(
        &[row(GENERATION, true, true, true)],
        GENERATION
    ));
}

#[test]
fn newer_active_or_materialized_execution_supersedes() {
    for newer in [
        row(GENERATION + 1, true, false, false),
        row(GENERATION + 1, false, false, true),
        row(GENERATION + 1, false, true, true),
    ] {
        assert!(
            execution_generation_superseded(
                &[row(GENERATION, false, false, true), newer],
                GENERATION
            ),
            "{newer:?}"
        );
    }
}

#[test]
fn newer_claim_that_never_ran_does_not_supersede() {
    assert!(!execution_generation_superseded(
        &[
            row(GENERATION, false, false, true),
            row(GENERATION + 1, false, true, false),
        ],
        GENERATION
    ));
}

#[test]
fn older_rows_are_ignored() {
    assert!(!execution_generation_superseded(
        &[
            row(GENERATION - 1, true, false, true),
            row(GENERATION - 2, false, true, true),
        ],
        GENERATION
    ));
}
