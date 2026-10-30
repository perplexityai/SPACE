//! The SQL the state checks read: the tables a catalog's migrations leave,
//! and the write statements in Rust string literals and `.sql` files.

use std::{collections::BTreeMap, sync::LazyLock};

use regex::Regex;

use crate::{
    baseline::line_of,
    context::{Dialect, Identifier, TableName, TextFile, quoted_identifier},
};

/// Where a live table is created: the migration and the 1-based line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Created {
    pub path: String,
    pub line: usize,
}

/// The tables `migrations` leave, applied in order onto `tables`: `CREATE
/// TABLE` adds one, unless it is temporary, `DROP TABLE` removes each it
/// lists, and `ALTER TABLE … RENAME TO` renames one, in its schema; a
/// statement on a temporary table changes none. Comments, strings and
/// `$tag$` bodies are not read, so DDL inside a function or a `COMMENT ON …
/// IS '…'` is not either; each is read as [`blank`] says for `dialect`.
/// Each table is keyed as [`TableName::key`] says for a database of
/// `dialect`.
pub(super) fn replay(
    migrations: &[TextFile],
    dialect: Dialect,
    tables: &mut BTreeMap<String, Created>,
) {
    for migration in migrations {
        let text = blank(&migration.text, true, dialect);
        let tokens = tokens(&text, dialect);
        let mut index = 0;
        while index < tokens.len() {
            index += statement(&tokens, index, migration, dialect, tables).max(1);
        }
    }
}

/// A token of SQL with comments, strings and bodies blanked: a word, with
/// its ASCII letters folded to lower case, a quoted identifier as written,
/// or one punctuation mark.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Quoted(String),
    Punct(char),
}

/// Whether `name` is in a schema of temporary tables: SQLite's `temp`, or
/// Postgres's `pg_temp` or a `pg_temp_<n>`.
fn is_temporary(name: &TableName, dialect: Dialect) -> bool {
    let Some(schema) = &name.schema else {
        return false;
    };
    let schema = dialect.fold(&schema.text, schema.quoted);
    match dialect {
        Dialect::Sqlite => schema == "temp",
        Dialect::Postgres => schema == "pg_temp" || schema.starts_with("pg_temp_"),
    }
}

/// Applies the DDL statement starting at token `at`, if it is one the
/// replay follows, and returns how many tokens it took (0 if none).
fn statement(
    tokens: &[(Token, usize)],
    at: usize,
    migration: &TextFile,
    dialect: Dialect,
    tables: &mut BTreeMap<String, Created>,
) -> usize {
    let word = |offset: usize| match tokens.get(at + offset) {
        Some((Token::Word(word), _)) => Some(word.as_str()),
        _ => None,
    };
    let mut next = 1;
    let skip = |words: &[&str], next: &mut usize| {
        if words.iter().any(|candidate| word(*next) == Some(candidate)) {
            *next += 1;
        }
    };
    match word(0) {
        Some("create") => {
            skip(&["global", "local"], &mut next);
            let temporary = matches!(word(next), Some("temp" | "temporary"));
            skip(&["temp", "temporary", "unlogged"], &mut next);
            if word(next) != Some("table") {
                return 1;
            }
            next += 1;
            if_exists(&word, &mut next, true);
            let Some((name, len)) = name(tokens, at + next) else {
                return next;
            };
            if !temporary && !is_temporary(&name, dialect) {
                let offset = tokens.get(at + next).map_or(0, |(_, offset)| *offset);
                let line = line_of(&migration.text, offset);
                tables.entry(name.key(dialect)).or_insert_with(|| Created {
                    path: migration.path.clone(),
                    line,
                });
            }
            next + len
        }
        Some("drop") => {
            if word(next) != Some("table") {
                return 1;
            }
            next += 1;
            if_exists(&word, &mut next, false);
            while let Some((name, len)) = name(tokens, at + next) {
                if !is_temporary(&name, dialect) {
                    tables.remove(&name.key(dialect));
                }
                next += len;
                if tokens.get(at + next).map(|(token, _)| token) != Some(&Token::Punct(',')) {
                    break;
                }
                next += 1;
            }
            next
        }
        Some("alter") => {
            if word(next) != Some("table") {
                return 1;
            }
            next += 1;
            if_exists(&word, &mut next, false);
            skip(&["only"], &mut next);
            let Some((old, len)) = name(tokens, at + next) else {
                return next;
            };
            next += len;
            if word(next) != Some("rename") || word(next + 1) != Some("to") {
                return next;
            }
            next += 2;
            let Some((mut new, len)) = name(tokens, at + next) else {
                return next;
            };
            // A renamed table stays in its schema.
            if new.schema.is_none() {
                new.schema = old.schema.clone();
            }
            if !is_temporary(&old, dialect)
                && let Some(created) = tables.remove(&old.key(dialect))
            {
                tables.insert(new.key(dialect), created);
            }
            next + len
        }
        _ => 1,
    }
}

/// Moves `next` past `IF NOT EXISTS` (`not`) or `IF EXISTS`.
fn if_exists<'a>(word: &impl Fn(usize) -> Option<&'a str>, next: &mut usize, not: bool) {
    let words: &[&str] = if not {
        &["if", "not", "exists"]
    } else {
        &["if", "exists"]
    };
    if words
        .iter()
        .enumerate()
        .all(|(offset, expected)| word(*next + offset) == Some(expected))
    {
        *next += words.len();
    }
}

/// The table name at token `at`, its parts joined by `.`s, and how many
/// tokens it takes.
fn name(tokens: &[(Token, usize)], at: usize) -> Option<(TableName, usize)> {
    let part = |index: usize| match tokens.get(index).map(|(token, _)| token) {
        Some(Token::Word(word)) => Some(Identifier {
            text: word.clone(),
            quoted: false,
        }),
        Some(Token::Quoted(name)) => Some(Identifier {
            text: name.clone(),
            quoted: true,
        }),
        _ => None,
    };
    let mut parts = vec![part(at)?];
    let mut len = 1;
    while tokens.get(at + len).map(|(token, _)| token) == Some(&Token::Punct('.')) {
        let Some(next) = part(at + len + 1) else {
            break;
        };
        parts.push(next);
        len += 2;
    }
    Some((TableName::of(parts)?, len))
}

/// The tokens of `text`, each with its byte offset, with identifiers
/// quoted as a database of `dialect` quotes them.
fn tokens(text: &str, dialect: Dialect) -> Vec<(Token, usize)> {
    let mut tokens = Vec::new();
    let mut at = 0;
    while let Some(rest) = text.get(at..) {
        let Some(c) = rest.chars().next() else {
            break;
        };
        if c.is_whitespace() {
            at += c.len_utf8();
        } else if c.is_alphanumeric() || c == '_' {
            let len = rest
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .unwrap_or(rest.len());
            let word = rest.get(..len).unwrap_or_default().to_ascii_lowercase();
            tokens.push((Token::Word(word), at));
            at += len;
        } else if let Some((name, len)) = quoted_identifier(rest, dialect) {
            tokens.push((Token::Quoted(name), at));
            at += len;
        } else {
            tokens.push((Token::Punct(c), at));
            at += c.len_utf8();
        }
    }
    tokens
}

/// `text` with every SQL comment and `'…'` string blanked, and with
/// `bodies`, every `$tag$…$tag$` body too; without, a body is read as the
/// code it is. Each blanked byte becomes a space and line breaks stay, so
/// offsets and lines are kept. A `--` or `/*` inside a string or a quoted
/// identifier (`"…"`, `` `…` ``, or SQLite's `[…]`) starts no comment. A
/// Postgres escape string, `E'…'`, ends at no `\'`. In SQLite, a `[`
/// quotes an identifier up to the first `]`, `'`s included; in Postgres,
/// `[` is an array's or a subscript's, such as `ARRAY['a']`, so the
/// comments and strings inside it are blanked as anywhere else.
pub(super) fn blank(text: &str, bodies: bool, dialect: Dialect) -> String {
    let bytes = text.as_bytes();
    let mut out = bytes.to_vec();
    let blank_range = |out: &mut Vec<u8>, from: usize, to: usize| {
        for byte in out.iter_mut().take(to).skip(from) {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    };
    let mut at = 0;
    while let Some(rest) = bytes.get(at..).filter(|rest| !rest.is_empty()) {
        let from = |index: usize| rest.get(index..).unwrap_or_default();
        let len = if rest.starts_with(b"--") {
            let len = rest.iter().position(|b| *b == b'\n').unwrap_or(rest.len());
            blank_range(&mut out, at, at + len);
            len
        } else if rest.starts_with(b"/*") {
            let mut depth = 0;
            let mut index = 0;
            while index < rest.len() {
                if from(index).starts_with(b"/*") {
                    depth += 1;
                    index += 2;
                } else if from(index).starts_with(b"*/") {
                    depth -= 1;
                    index += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    index += 1;
                }
            }
            blank_range(&mut out, at, at + index);
            index
        } else if rest.first() == Some(&b'[') && dialect.has_bracket_identifiers() {
            rest.iter()
                .position(|b| *b == b']')
                .map_or(1, |close| close + 1)
        } else if let Some(quote) = rest
            .first()
            .copied()
            .filter(|b| matches!(b, b'\'' | b'"' | b'`'))
        {
            let escapes = quote == b'\'' && is_escape_string(bytes, at);
            let mut index = 1;
            while let Some(byte) = rest.get(index) {
                index += 1;
                if escapes && *byte == b'\\' {
                    index = (index + 1).min(rest.len());
                } else if *byte == quote {
                    if rest.get(index) != Some(&quote) {
                        break;
                    }
                    index += 1;
                }
            }
            if quote == b'\'' {
                blank_range(&mut out, at, at + index);
            }
            index
        } else if let Some(tag) = dollar_tag(rest) {
            if bodies {
                let close =
                    find(from(tag.len()), tag).map_or(rest.len(), |end| 2 * tag.len() + end);
                blank_range(&mut out, at, at + close);
                close
            } else {
                tag.len()
            }
        } else {
            1
        };
        at += len;
    }
    // Only ASCII bytes were replaced, each by a space, so whole characters
    // were.
    String::from_utf8(out).unwrap_or_else(|_| text.to_owned())
}

/// Whether the `'` at `at` in `text` opens a Postgres escape string: an `E`
/// before it that ends no longer word.
fn is_escape_string(text: &[u8], at: usize) -> bool {
    let before = |back: usize| at.checked_sub(back).and_then(|index| text.get(index));
    let word = |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'$') || !b.is_ascii();
    before(1).is_some_and(|b| b.eq_ignore_ascii_case(&b'e')) && !before(2).is_some_and(word)
}

/// The `$tag$` that `text` starts with, if it does.
fn dollar_tag(text: &[u8]) -> Option<&[u8]> {
    let name = text.strip_prefix(b"$")?;
    let len = name
        .iter()
        .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_'))?;
    // `$1` is a parameter, not a tag.
    let starts_with_digit = name.first().is_some_and(u8::is_ascii_digit);
    if name.get(len) != Some(&b'$') || starts_with_digit {
        return None;
    }
    text.get(..len + 2)
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// A write statement's head and the table it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Write {
    /// The head as written, such as `UPDATE` or `DELETE FROM`, with its
    /// whitespace collapsed.
    pub head: String,
    pub table: TableName,
    /// The byte offset of the head in the text read: for
    /// [`rust_writes`], the Rust source.
    pub at: usize,
}

/// A head keyword sequence, in any case: `INSERT [OR x] INTO`, `REPLACE
/// INTO`, `UPDATE [OR x]`, `DELETE FROM`, `MERGE INTO` or `TRUNCATE [TABLE]`,
/// then an optional `ONLY`.
static HEAD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(insert(?:\s+or\s+[a-z]+)?\s+into|replace\s+into|update(?:\s+or\s+(?:abort|fail|ignore|replace|rollback))?|delete\s+from|merge\s+into|truncate(?:\s+table)?)\s+(?:only\s+)?",
    )
    .expect("the head pattern is valid")
});

/// Every write statement head in `text`, SQL with its comments and strings
/// blanked, and the table each names, read as a database of `dialect`
/// reads names; a `TRUNCATE` names each table it lists. A head right after
/// a word character or a `.` is not one.
pub(super) fn writes(text: &str, dialect: Dialect) -> Vec<Write> {
    let mut writes = Vec::new();
    for found in HEAD.captures_iter(text) {
        let (Some(whole), Some(head)) = (found.get(0), found.get(1)) else {
            continue;
        };
        if text
            .get(..whole.start())
            .is_some_and(|before| before.ends_with('.'))
        {
            continue;
        }
        let head_text: String = head
            .as_str()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_uppercase();
        let truncate = head_text.starts_with("TRUNCATE");
        let mut rest = text.get(whole.end()..).unwrap_or_default();
        while let Some((table, len)) = TableName::parse(rest, dialect) {
            writes.push(Write {
                head: head_text.clone(),
                table,
                at: head.start(),
            });
            let next = rest.get(len..).unwrap_or_default().trim_start();
            match next.strip_prefix(',') {
                Some(list) if truncate => rest = list.trim_start(),
                _ => break,
            }
        }
    }
    writes
}

/// A Rust string literal's text, decoded, and the byte offset in the file of
/// each of its bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Literal {
    pub text: String,
    pub offsets: Vec<usize>,
}

impl Literal {
    fn push(&mut self, c: char, at: usize) {
        let before = self.text.len();
        self.text.push(c);
        self.offsets
            .extend(std::iter::repeat_n(at, self.text.len() - before));
    }
}

/// Every string literal of Rust source `text`, in order: `"…"` with its
/// escapes and line continuations decoded, raw `r#"…"#`, and their `b` and
/// `c` forms. Comments, char and byte literals and lifetimes are skipped.
pub(super) fn literals(text: &str) -> Vec<Literal> {
    let mut literals = Vec::new();
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let at = |index: usize| chars.get(index).map(|(_, c)| *c);
    let mut index = 0;
    while let Some(c) = at(index) {
        match c {
            '/' if at(index + 1) == Some('/') => {
                while at(index).is_some_and(|c| c != '\n') {
                    index += 1;
                }
            }
            '/' if at(index + 1) == Some('*') => {
                let mut depth = 0;
                while let Some(c) = at(index) {
                    if c == '/' && at(index + 1) == Some('*') {
                        depth += 1;
                        index += 2;
                    } else if c == '*' && at(index + 1) == Some('/') {
                        depth -= 1;
                        index += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        index += 1;
                    }
                }
            }
            '"' => {
                let (literal, end) = quoted(&chars, index + 1);
                literals.push(literal);
                index = end;
            }
            '\'' => index = after_quote(&chars, index),
            c if c.is_alphabetic() || c == '_' => {
                let start = index;
                while at(index).is_some_and(|c| c.is_alphanumeric() || c == '_') {
                    index += 1;
                }
                let word: String = chars
                    .get(start..index)
                    .unwrap_or_default()
                    .iter()
                    .map(|(_, c)| c)
                    .collect();
                match (word.as_str(), at(index)) {
                    ("r" | "br" | "cr", Some('"' | '#')) => {
                        if let Some((literal, end)) = raw(&chars, index) {
                            literals.push(literal);
                            index = end;
                        }
                    }
                    ("b" | "c", Some('"')) => {
                        let (literal, end) = quoted(&chars, index + 1);
                        literals.push(literal);
                        index = end;
                    }
                    ("b", Some('\'')) => index = after_quote(&chars, index),
                    _ => {}
                }
            }
            _ => index += 1,
        }
    }
    literals
}

/// The index after the char or byte literal, or the lifetime's quote, at
/// `index`.
fn after_quote(chars: &[(usize, char)], index: usize) -> usize {
    let at = |index: usize| chars.get(index).map(|(_, c)| *c);
    if at(index + 1) == Some('\\') {
        // The escaped character, which may be a quote, is not the end.
        let mut end = index + 3;
        while at(end).is_some_and(|c| c != '\'') {
            end += 1;
        }
        return end + 1;
    }
    if at(index + 2) == Some('\'') {
        return index + 3;
    }
    index + 1
}

/// The `"…"` literal whose text starts at `index`, decoded, and the index
/// after its closing quote.
fn quoted(chars: &[(usize, char)], mut index: usize) -> (Literal, usize) {
    let at = |index: usize| chars.get(index).copied();
    let mut literal = Literal {
        text: String::new(),
        offsets: Vec::new(),
    };
    while let Some((offset, c)) = at(index) {
        index += 1;
        match c {
            '"' => break,
            '\\' => {
                let Some((_, escape)) = at(index) else { break };
                index += 1;
                let decoded = match escape {
                    'n' => Some('\n'),
                    'r' => Some('\r'),
                    't' => Some('\t'),
                    '0' => Some('\0'),
                    'x' => {
                        let hex: String = (index..index + 2)
                            .filter_map(|i| at(i).map(|(_, c)| c))
                            .collect();
                        index += 2;
                        u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                    }
                    'u' => {
                        let mut hex = String::new();
                        while let Some((_, c)) = at(index) {
                            index += 1;
                            match c {
                                '{' => {}
                                '}' => break,
                                c => hex.push(c),
                            }
                        }
                        u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32)
                    }
                    '\n' | '\r' => {
                        // A line continuation: the break and the next line's
                        // leading whitespace are not part of the text.
                        while at(index).is_some_and(|(_, c)| c.is_whitespace()) {
                            index += 1;
                        }
                        None
                    }
                    other => Some(other),
                };
                if let Some(decoded) = decoded {
                    literal.push(decoded, offset);
                }
            }
            c => literal.push(c, offset),
        }
    }
    (literal, index)
}

/// The raw literal whose `#`s or opening quote are at `index`, and the index
/// after it; `None` if no quote follows the `#`s.
fn raw(chars: &[(usize, char)], mut index: usize) -> Option<(Literal, usize)> {
    let at = |index: usize| chars.get(index).copied();
    let mut hashes = 0;
    while at(index).is_some_and(|(_, c)| c == '#') {
        hashes += 1;
        index += 1;
    }
    if at(index).map(|(_, c)| c) != Some('"') {
        return None;
    }
    index += 1;
    let mut literal = Literal {
        text: String::new(),
        offsets: Vec::new(),
    };
    while let Some((offset, c)) = at(index) {
        index += 1;
        let closes =
            c == '"' && (0..hashes).all(|offset| at(index + offset).is_some_and(|(_, c)| c == '#'));
        if closes {
            return Some((literal, index + hashes));
        }
        literal.push(c, offset);
    }
    Some((literal, index))
}

/// Every write head in the string literals of Rust source `text`, each
/// literal read as SQL of `dialect` with its comments and strings blanked,
/// as the head, its table and the 1-based line of the head in `text`.
pub(super) fn rust_writes(text: &str, dialect: Dialect) -> Vec<(Write, usize)> {
    literals(text)
        .into_iter()
        .flat_map(|literal| {
            let sql = blank(&literal.text, false, dialect);
            writes(&sql, dialect)
                .into_iter()
                .map(|mut write| {
                    write.at = literal.offsets.get(write.at).copied().unwrap_or_default();
                    let line = line_of(text, write.at);
                    (write, line)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Every write head in the SQL file text `text`, read as SQL of `dialect`
/// with its comments and strings blanked, with the 1-based line of each
/// head.
pub(super) fn sql_writes(text: &str, dialect: Dialect) -> Vec<(Write, usize)> {
    writes(&blank(text, false, dialect), dialect)
        .into_iter()
        .map(|write| {
            let line = line_of(text, write.at);
            (write, line)
        })
        .collect()
}
