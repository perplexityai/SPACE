//! Call-site locator comments for SQL statements.
//!
//! [`sql!`](crate::sql) prepends a `/*module_path:line*/` comment to a SQL
//! statement so server-side surfaces (`pg_stat_activity`, slow-query logs,
//! Datadog DBM samples) attribute an in-flight query back to the Rust code
//! that issued it. The comment is a single whitespace-free token, so it
//! survives `track_activity_query_size` truncation (it leads the statement)
//! and consumes only one word of sqlx's word-based query summary.
//!
//! It is its own crate, with no dependencies, so a crate that needs only the
//! locator does not depend on `pplx-space-common-core` for it.

/// Prepend a `/*module_path:line*/` locator comment to a SQL statement.
///
/// With a string literal the expansion is a `&'static str` (via `concat!`),
/// which satisfies sqlx's `SqlSafeStr` and drops into `sqlx::query*` calls
/// unchanged. With any other expression the statement is built at runtime and
/// the expansion is a `String`; wrap it in `sqlx::AssertSqlSafe` exactly as
/// for any other runtime-built SQL.
///
/// # Examples
///
/// ```rust
/// use pplx_space_sql_locator::sql;
///
/// let static_sql: &'static str = sql!("SELECT 1");
/// assert!(static_sql.starts_with("/*"));
/// assert!(static_sql.ends_with("*/ SELECT 1"));
///
/// let table = "sandboxes";
/// let dynamic_sql: String = sql!(format!("SELECT count(*) FROM {table}"));
/// assert!(dynamic_sql.ends_with("*/ SELECT count(*) FROM sandboxes"));
/// ```
#[macro_export]
macro_rules! sql {
    ($sql:literal $(,)?) => {
        concat!("/*", module_path!(), ":", line!(), "*/ ", $sql)
    };
    ($sql:expr $(,)?) => {
        format!("/*{}:{}*/ {}", module_path!(), line!(), $sql)
    };
}

#[cfg(test)]
mod tests;
