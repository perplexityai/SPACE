use super::*;

fn params() -> PageWalkParams {
    PageWalkParams {
        cursor_created_at: "$1",
        cursor_id: "$2",
        max_rows: "$3",
        max_bytes: "$4",
        row_overhead_bytes: "$5",
        lists_env: false,
    }
}

#[test]
fn the_page_walk_fills_in_the_cursor_and_the_bound() {
    let sql = page_rows_cte(
        "SELECT s.id, s.created_at FROM sandboxes s WHERE {after_cursor} LIMIT {max_rows}",
        &params(),
    );
    assert!(
        !sql.contains("{after_cursor}") && !sql.contains("{max_rows}"),
        "{sql}"
    );
    assert!(sql.contains("LIMIT $3::BIGINT"), "{sql}");
}

#[test]
#[should_panic(expected = "candidate rows must write {after_cursor} and {max_rows}")]
fn candidate_rows_without_the_cursor_fail_loudly() {
    page_rows_cte(
        "SELECT s.id, s.created_at FROM sandboxes s LIMIT {max_rows}",
        &params(),
    );
}

#[test]
#[should_panic(expected = "candidate rows must write {after_cursor} and {max_rows}")]
fn candidate_rows_without_the_bound_fail_loudly() {
    page_rows_cte(
        "SELECT s.id, s.created_at FROM sandboxes s WHERE {after_cursor}",
        &params(),
    );
}
