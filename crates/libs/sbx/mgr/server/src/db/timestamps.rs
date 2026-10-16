//! Manager timestamps use whole, nonnegative Unix seconds, regardless of SQLite affinity.
//! Historical SQLite UTC and RFC3339 text remain readable during rolling upgrades.

use std::fmt;

use crate::db::{Connection, Result};
use rusqlite::params;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, Value, ValueRef};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EpochSeconds(i64);

#[derive(Debug, thiserror::Error)]
#[error("expected whole nonnegative Unix seconds or a whole-second UTC timestamp")]
pub(crate) struct InvalidTimestamp;

impl EpochSeconds {
    pub(crate) fn parse(text: &str) -> std::result::Result<Self, InvalidTimestamp> {
        if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
            return text.parse().map(Self).map_err(|_| InvalidTimestamp);
        }
        let timestamp = text.parse::<jiff::Timestamp>().ok().or_else(|| {
            jiff::civil::DateTime::strptime("%Y-%m-%d %H:%M:%S", text)
                .ok()?
                .to_zoned(jiff::tz::TimeZone::UTC)
                .ok()
                .map(|value| value.timestamp())
        });
        timestamp
            .filter(|value| value.as_second() >= 0 && value.as_nanosecond() % 1_000_000_000 == 0)
            .map(|value| Self(value.as_second()))
            .ok_or(InvalidTimestamp)
    }

    pub(in crate::db) fn decode(value: &Value) -> FromSqlResult<Self> {
        Self::column_result(ValueRef::from(value))
    }

    pub(crate) fn get(self) -> i64 {
        self.0
    }

    pub(crate) fn unsigned(self) -> u64 {
        self.0.cast_unsigned()
    }
}

impl FromSql for EpochSeconds {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(value) if value >= 0 => Ok(Self(value)),
            ValueRef::Text(value) => std::str::from_utf8(value)
                .ok()
                .and_then(|value| Self::parse(value).ok())
                .ok_or_else(|| FromSqlError::Other(Box::new(InvalidTimestamp))),
            _ => Err(FromSqlError::Other(Box::new(InvalidTimestamp))),
        }
    }
}

impl fmt::Display for EpochSeconds {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Diagnostic paths preserve corrupt text for degraded reporting instead of
/// making a malformed timestamp hide the sandbox's raw lifecycle state.
pub(in crate::db) fn diagnostic_text(value: Value) -> Option<String> {
    match EpochSeconds::decode(&value) {
        Ok(value) => Some(value.to_string()),
        Err(_) => match value {
            Value::Text(value) => Some(value),
            Value::Integer(value) => Some(value.to_string()),
            _ => None,
        },
    }
}

pub(super) fn diagnostic_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<String> {
    let value: Value = row.get(index)?;
    let data_type = value.data_type();
    diagnostic_text(value).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(index, data_type, Box::new(InvalidTimestamp))
    })
}

/// Convert the only manager table historically written with SQLite's UTC text
/// default. A fixed rowid ceiling prevents new scopes extending a sweep forever;
/// malformed evidence is retained and cannot prevent later rows being visited.
#[derive(Debug)]
pub(crate) struct RestoreScopeTimestampScan {
    next: i64,
    through: Option<i64>,
}

impl Default for RestoreScopeTimestampScan {
    fn default() -> Self {
        Self {
            next: i64::MIN,
            through: None,
        }
    }
}

impl RestoreScopeTimestampScan {
    pub(crate) fn next_batch(&mut self, conn: &Connection) -> Result<usize> {
        let result = self.normalize_batch(conn);
        if result.is_err() {
            *self = Self::default();
        }
        result
    }

    fn normalize_batch(&mut self, conn: &Connection) -> Result<usize> {
        let tx = super::immediate_transaction(conn)?;
        if self.through.is_none() {
            self.through = tx.query_row(
                "SELECT MAX(rowid) FROM sandbox_restore_transfer_scopes",
                [],
                |row| row.get(0),
            )?;
        }
        let Some(through) = self.through else {
            return Ok(0);
        };
        let rows = {
            let mut statement = tx.prepare_cached(
                "SELECT rowid, created_at FROM sandbox_restore_transfer_scopes
                 WHERE rowid >= ?1 AND rowid <= ?2 ORDER BY rowid LIMIT 128",
            )?;
            statement
                .query_map(params![self.next, through], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, Value>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut changed = 0;
        for (rowid, raw) in &rows {
            // Decimal TEXT already uses the right units. Do not rewrite it for
            // cosmetic differences such as leading zeroes or storage affinity.
            if let Value::Text(text) = raw
                && text.contains('-')
                && let Ok(timestamp) = EpochSeconds::decode(raw)
            {
                changed += tx.execute(
                    "UPDATE sandbox_restore_transfer_scopes SET created_at = ?1
                     WHERE rowid = ?2 AND created_at IS ?3",
                    params![timestamp.get(), rowid, raw],
                )?;
            }
        }
        tx.commit()?;
        match rows.last() {
            Some((last, _)) if rows.len() == 128 && *last < through => self.next = last + 1,
            _ => *self = Self::default(),
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests;
