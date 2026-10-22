#[test]
fn literal_form_is_static_and_carries_module_and_line() {
    let expected_line = line!() + 1;
    let sql: &'static str = crate::sql!("SELECT 1");
    assert_eq!(
        sql,
        format!("/*pplx_space_sql_locator::tests:{expected_line}*/ SELECT 1")
    );
}

#[test]
fn locator_comment_is_a_single_whitespace_free_token() {
    let sql = crate::sql!("SELECT 1");
    let (comment, _) = sql.split_once("*/").expect("comment terminator");
    assert!(!comment.contains(char::is_whitespace));
}

#[test]
fn expr_form_prepends_locator_to_runtime_sql() {
    let table = "sandboxes";
    let expected_line = line!() + 1;
    let sql: String = crate::sql!(format!("DELETE FROM {table}"));
    assert_eq!(
        sql,
        format!(
            "/*pplx_space_sql_locator::tests:{expected_line}*/ \
                 DELETE FROM sandboxes"
        )
    );
}
