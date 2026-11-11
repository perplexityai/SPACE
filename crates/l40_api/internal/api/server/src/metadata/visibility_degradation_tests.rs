use super::*;

/// `42703` is the *only* SQLSTATE that may be reported as `column_missing`.
///
/// This is the assertion that makes the constant load-bearing: point
/// `SQLSTATE_UNDEFINED_COLUMN` at any other code and this test fails. Without
/// it the mapping is silently mutable — the RPC-level tests only observe the
/// resulting `Unspecified`, which is identical on both branches, so they pass
/// either way while the schema-drift `column_missing` signal never fires and a
/// live DB incident gets logged as schema drift.
#[test]
fn only_undefined_column_is_attributed_to_schema_skew() {
    assert_eq!(
        degradation_for_sqlstate(Some("42703")),
        VisibilityLookupDegradation::ColumnMissing
    );
}

/// Every other failure class — including codes adjacent to `42703` in the
/// `42xxx` syntax/access family, resource exhaustion, permission denial, and
/// the `0A000` a rolled-back migration can surface through a cached plan —
/// must read as a live query failure, not as `column_missing`.
#[test]
fn other_sqlstates_are_attributed_to_query_failure() {
    for code in [
        "42704", // undefined_object — one digit from undefined_column
        "42P01", // undefined_table
        "42501", // insufficient_privilege
        "53300", // too_many_connections
        "57014", // query_canceled (statement_timeout)
        "55P03", // lock_not_available
        "0A000", // cached plan must not change result type
    ] {
        assert_eq!(
            degradation_for_sqlstate(Some(code)),
            VisibilityLookupDegradation::QueryFailed,
            "SQLSTATE {code} must not be attributed to schema skew"
        );
    }
}

/// A non-database error (pool acquire timeout, connection reset, decode
/// failure) carries no SQLSTATE at all. It is still a real failure, so it must
/// not fall through to the `column_missing` schema-drift story.
#[test]
fn absent_sqlstate_is_attributed_to_query_failure() {
    assert_eq!(
        degradation_for_sqlstate(None),
        VisibilityLookupDegradation::QueryFailed
    );
}

/// The two questions must be answered separately, because their fail-safe
/// directions are opposite.
///
/// An unclassified SQLSTATE must **not** serve a cached read (fail closed on the
/// answer) and must **not** drop it either. Otherwise an unlisted code can
/// erase the entry, leaving subsequent lock contention (`57014`) without a
/// cached value to serve.
///
/// `XX000 internal_error` is the realistic instance — Postgres raises it for
/// "could not open relation" and "cache lookup failed" under concurrent DDL,
/// which is exactly the window this lookup is exposed to.
///
/// `58P01`/`58P02` (`undefined_file`, `duplicate_file`) are a missing relation
/// segment, i.e. corruption that persists until a restore. Serving through it is
/// wrong; treating it as evidence about the row's *content* is also wrong. Note
/// they are not interchangeable with `58030 io_error`, which is a momentary
/// storage blip and is on the transient list.
#[test]
fn an_unclassified_failure_neither_serves_nor_erases() {
    for code in ["XX000", "22P02", "42601", "99999", "58P01", "58P02"] {
        assert!(
            !may_serve_stale(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: an unrecognized code must not serve a cached read"
        );
        assert!(
            !should_invalidate(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: an unrecognized code must not erase the cached read either"
        );
    }
}

/// A positively-identified durable condition does drop the entry, so the split
/// above cannot be satisfied by never invalidating at all.
#[test]
fn a_durable_failure_erases_the_cached_read() {
    for code in ["42703", "42P01", "42501", "3D000", "57P04"] {
        assert!(
            should_invalidate(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: a durable condition must drop the cached read"
        );
        assert!(
            !may_serve_stale(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: and must not serve it"
        );
    }
}

/// A momentary failure keeps the entry *and* may serve it — the case the cache
/// exists for.
///
/// `57P04 database_dropped` sharing class `57` with three of these is why that
/// class is enumerated member-by-member rather than by prefix; `58030 io_error`
/// is here because a storage blip is momentary, and it was the code that exposed
/// the shared-predicate defect.
///
/// `0A000` is the one that had to be argued for, having been on all three lists
/// across revisions — the reasoning lives on the constant. The short version: it
/// describes a *connection's* stale plan, not the row, which is invariant 2's
/// serve-stale condition verbatim; and because nothing in `sqlx` re-prepares
/// after it, refusing the fallback failed the replica closed with a valid entry
/// in hand until the pool reclaimed the connection — 5 minutes if it went idle,
/// up to ~35 if it stayed busy. Deliberately not stated as "a `max_lifetime`":
/// `postgres_pool_options` sets that knob to `None` and moves retirement to an
/// `after_release` age check against a jittered lifetime, so naming the knob
/// would attribute the bound to the layer that is switched off.
#[test]
fn a_momentary_failure_keeps_and_serves_the_cached_read() {
    for code in [
        "57014", "57P01", "57P03", "08006", "53300", "40001", "58030", "0A000",
    ] {
        assert!(
            !should_invalidate(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: a momentary failure must not drop the cached read"
        );
        assert!(
            may_serve_stale(&sqlx::Error::PoolTimedOut, Some(code)),
            "{code}: a momentary failure may serve the cached read"
        );
    }
    // No SQLSTATE at all is the transport shape — a closed or exhausted pool
    // never reaches the server.
    assert!(may_serve_stale(&sqlx::Error::PoolTimedOut, None));
    assert!(!should_invalidate(&sqlx::Error::PoolClosed, None));
}

/// A row that was read and cannot be decoded is invariant 2's case arriving
/// through `Err`: it both refuses the fallback and drops the entry, because the
/// row's answer changed to something the cache must not paper over.
///
/// Every variant in `read_but_unusable` is exercised, not just the reachable
/// pair. The three refactor-safety members are the ones with no other guard:
/// deleting any of them from the `matches!` would otherwise leave the suite
/// green, and `ColumnIndexOutOfBounds` is both the least obvious and the one its
/// own doc calls "the one a change to *this* statement's column list would
/// raise" — the most likely to be dropped by someone editing the query.
#[test]
fn a_read_but_unusable_row_both_refuses_and_erases() {
    let decode = sqlx::Error::ColumnDecode {
        index: "visibility".to_string(),
        source: "mismatched types".into(),
    };
    let plain_decode = sqlx::Error::Decode("mismatched types".into());
    let index_out_of_bounds = sqlx::Error::ColumnIndexOutOfBounds { index: 2, len: 2 };
    let column_not_found = sqlx::Error::ColumnNotFound("visibility".to_string());
    for error in [
        &decode,
        &plain_decode,
        &index_out_of_bounds,
        &column_not_found,
        &sqlx::Error::RowNotFound,
    ] {
        assert!(!may_serve_stale(error, None));
        assert!(should_invalidate(error, None));
    }
}

/// The transient and durable sets must stay disjoint.
///
/// If a code landed in both, `should_invalidate` and `may_serve_stale` would both
/// be true, and the call site invalidates *before* consulting the fallback — so
/// it would erase the entry and then find nothing to serve. Fail-safe, but it
/// silently disables the cache for that code with no test firing. Asserted over
/// the constants rather than a hand-copied list so adding a code to either side
/// cannot introduce the overlap unnoticed.
#[test]
fn the_transient_and_durable_sets_are_disjoint() {
    for durable in DURABLE_SQLSTATES {
        assert!(
            !TRANSIENT_SQLSTATES.contains(durable),
            "{durable} is in both the transient and durable lists"
        );
    }
    // Classification by prefix was removed because every candidate class held a
    // standing member; a reintroduced prefix would reopen that. Assert the shape
    // rather than the absence, so a two-character entry cannot be added quietly.
    for code in TRANSIENT_SQLSTATES.iter().chain(DURABLE_SQLSTATES) {
        assert_eq!(
            code.len(),
            5,
            "{code} is not a full 5-character SQLSTATE; classifying by prefix was \
             removed deliberately"
        );
    }
}

/// A pre-execution failure must not erase the cached read.
///
/// `TypeNotFound` is the concrete one, and it was on the erase side for several
/// revisions. Its only raise site is type-OID resolution
/// (`sqlx-postgres-0.9.0/src/connection/resolve.rs:189`), which runs *before* the
/// statement does, so nothing has been learned about the row — the same reason
/// `58P01`/`58P02` are left unclassified rather than durable. Erasing on it would
/// drop a valid entry during exactly the named-type migration that is also
/// producing the lock waits the fallback exists to cover.
///
/// Asserted over the *class*, not one variant. An earlier version of this test
/// checked `TypeNotFound` alone while claiming the class property, which is the
/// gap its siblings avoid by looping: adding `Configuration` to
/// `read_but_unusable` would violate the stated property and leave a
/// single-variant test green.
#[test]
fn a_pre_execution_failure_does_not_erase_the_cached_read() {
    let pre_execution = [
        sqlx::Error::TypeNotFound {
            type_name: "template_visibility".to_string(),
        },
        sqlx::Error::Configuration("bad connection string".into()),
        sqlx::Error::Encode("cannot encode the bind parameter".into()),
        sqlx::Error::Protocol("unexpected backend message".to_string()),
    ];
    for error in &pre_execution {
        assert!(
            !should_invalidate(error, None),
            "{error:?}: a failure raised before the statement ran must not drop \
             the cached read"
        );
        assert!(
            may_serve_stale(error, None),
            "{error:?}: it falls to the no-SQLSTATE arm, which serves the last \
             successful read"
        );
        assert!(
            !read_but_unusable(error),
            "{error:?}: it is not a read-but-unusable row — no row was read"
        );
    }
}
