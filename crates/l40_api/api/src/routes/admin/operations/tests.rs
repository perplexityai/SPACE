use super::*;

/// `GET /admin/api/v1/operations` and `GET /.../{id}` take the lock wait as a
/// retryable 503 and never take the write conflict.
///
/// Both went through one arm that turned every store error into a 500, so an
/// operator polling a delete's operation row while a migration held
/// `ACCESS EXCLUSIVE` on `public_operations` saw an internal error for a wait
/// the identical retry survives. The `list` arm was not in this issue's site
/// table — it came out of walking every function that returns a database error
/// rather than grepping the mapper's shape.
///
/// Only what this wrapper adds is asserted here: that it pins the scope to
/// `ReadOnly`, so a serialization abort takes the generic retryable 503 rather
/// than the write-specific 409. The SQLSTATE table itself lives once in
/// `routes::admin::tests::a_read_only_statement_is_never_blamed_for_a_write_interleave`.
///
/// Asserts the wrapper, not the handler arm that calls it, so it does not catch
/// that call being dropped — only
/// `routes::debug::sandbox::tests::the_debug_handler_answers_a_contended_read_with_a_503`
/// and
/// `controllers::sandbox::lifecycle::tests::a_contended_delete_precheck_stays_retryable`
/// drive production code far enough for that.
#[tokio::test]
async fn an_operations_read_pins_the_scope_to_read_only()
-> pplx_space_db_test_fixtures::TestDbResult<()> {
    let db = pplx_space_db_test_fixtures::pg_test_db().await?;
    let wrap = PublicOperationStoreError::Sql;

    let lock_wait = wrap(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await);
    assert_eq!(
        store_error(&lock_wait).0,
        StatusCode::SERVICE_UNAVAILABLE,
        "a lock wait on an operations read is retryable, not internal"
    );

    let deadlock = wrap(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "40P01").await);
    assert_eq!(
        store_error(&deadlock).0,
        StatusCode::SERVICE_UNAVAILABLE,
        "these handlers wrote nothing, so the 409 would misreport them — but the \
         abort is still retryable"
    );

    // Outside the contention set, so still an internal error.
    let violation = wrap(pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await);
    assert_eq!(store_error(&violation).0, StatusCode::INTERNAL_SERVER_ERROR);

    // Closed like the sibling `raise_sqlstate` tests: each injection fails a
    // statement on the connection it borrows.
    db.pool.close().await;
    Ok(())
}

/// The operations filters are validated with the strict `parse_known` parsers.
///
/// `OperationState` and `OperationTargetType` are open response enums, so
/// their `FromStr` is infallible: an unknown value decodes as `Other(_)`
/// instead of failing. Routed through a generic `value.parse::<T>()`, that
/// turned `?state=bogus` from a 400 into an empty 200 — a filter that could
/// never match anything, reported as "no operations". Request input has to
/// take the strict parser and keep rejecting what this build does not know.
#[test]
fn unknown_filter_values_are_rejected_not_matched_against_nothing() {
    fn query(
        state: Option<&str>,
        kind: Option<&str>,
        target_type: Option<&str>,
    ) -> ListAdminOperationsQuery {
        ListAdminOperationsQuery {
            limit: None,
            org_id: None,
            state: state.map(str::to_owned),
            kind: kind.map(str::to_owned),
            target_type: target_type.map(str::to_owned),
            target_id: None,
        }
    }

    let known = query(Some("succeeded"), Some("create_sandbox"), Some("sandbox"))
        .into_store_input()
        .expect("known filter values parse");
    assert_eq!(known.state, Some(OperationState::Succeeded));
    assert_eq!(known.kind, Some(OperationKind::CreateSandbox));
    assert_eq!(known.target_type, Some(OperationTargetType::Sandbox));

    let none = query(None, None, None)
        .into_store_input()
        .expect("absent filters are not validated");
    assert_eq!(
        (none.state, none.kind, none.target_type),
        (None, None, None)
    );

    for (query, field) in [
        (query(Some("bogus"), None, None), "state"),
        (query(None, Some("bogus"), None), "kind"),
        (query(None, None, Some("bogus")), "target_type"),
    ] {
        let err = query
            .into_store_input()
            .err()
            .unwrap_or_else(|| panic!("an unknown {field} must be a 400, not an empty match"));
        assert_eq!(err.message, format!("invalid {field}"));
    }
}
