//! The lexer [`super`] reads files with: a Rust file's identifiers,
//! punctuation, literals and lifetimes, with the line each starts on.
//! Comments and whitespace are dropped, and a literal is one token, so text
//! inside comments and string, character and byte literals never reads as
//! code.

/// What a token is; only identifiers and punctuation are ever compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// An identifier or keyword, without the `r#` of a raw identifier.
    Ident,
    /// `::`, or one punctuation character.
    Punct,
    /// A string, character, byte or number literal.
    Literal,
    /// A lifetime or loop label.
    Lifetime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Token<'a> {
    pub kind: Kind,
    pub text: &'a str,
    /// The 1-based line it starts on.
    pub line: usize,
}

impl<'a> Token<'a> {
    pub(super) fn is(&self, punct: &str) -> bool {
        self.kind == Kind::Punct && self.text == punct
    }

    pub(super) fn ident(&self) -> Option<&'a str> {
        (self.kind == Kind::Ident).then_some(self.text)
    }
}

/// `text`'s tokens, without comments and whitespace. Anything that is not
/// valid Rust still lexes, so a file that does not compile is read as far
/// as it can be.
pub(super) fn lex(text: &str) -> Vec<Token<'_>> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut line = 1;
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let next = bytes.get(at + 1).copied();
        let (kind, start, end) = match byte {
            b'\n' => {
                line += 1;
                at += 1;
                continue;
            }
            _ if byte.is_ascii_whitespace() => {
                at += 1;
                continue;
            }
            b'/' if next == Some(b'/') => {
                at = find(bytes, at, b"\n").unwrap_or(bytes.len());
                continue;
            }
            b'/' if next == Some(b'*') => {
                let end = block_comment_end(bytes, at);
                line += newlines(bytes, at, end);
                at = end;
                continue;
            }
            b'"' => (Kind::Literal, at, quoted_end(bytes, at + 1, b'"')),
            b'\'' => {
                let (kind, end) = quote(text, at);
                (kind, at, end)
            }
            b'0'..=b'9' => (Kind::Literal, at, ident_end(text, at)),
            b':' if next == Some(b':') => (Kind::Punct, at, at + 2),
            _ => match rest(text, at).chars().next() {
                Some(c) if is_ident_start(c) => word(text, at),
                Some(c) => (Kind::Punct, at, at + c.len_utf8()),
                None => break,
            },
        };
        tokens.push(Token {
            kind,
            text: text.get(start..end).unwrap_or_default(),
            line,
        });
        line += newlines(bytes, at, end);
        at = end.max(at + 1);
    }
    tokens
}

/// `text` from byte `at`, or nothing if `at` is not a character boundary
/// inside it.
fn rest(text: &str, at: usize) -> &str {
    text.get(at..).unwrap_or_default()
}

/// The first index at or after `from` where `bytes` has `needle`.
fn find(bytes: &[u8], from: usize, needle: &[u8]) -> Option<usize> {
    bytes
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| from + offset)
}

/// The line breaks in `bytes[from..to]`.
fn newlines(bytes: &[u8], from: usize, to: usize) -> usize {
    bytes
        .get(from..to.min(bytes.len()))
        .unwrap_or_default()
        .iter()
        .filter(|&&byte| byte == b'\n')
        .count()
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_alphabetic()
}

/// The end of the identifier, keyword or number at `at`.
fn ident_end(text: &str, at: usize) -> usize {
    rest(text, at)
        .find(|c: char| !(c == '_' || c.is_alphanumeric()))
        .map_or(text.len(), |offset| at + offset)
}

/// The end of a nested block comment starting at `at`.
fn block_comment_end(bytes: &[u8], at: usize) -> usize {
    let mut depth = 0;
    let mut i = at;
    while let Some(pair) = bytes.get(i..i + 2) {
        match pair {
            b"/*" => {
                depth += 1;
                i += 2;
            }
            b"*/" => {
                depth -= 1;
                i += 2;
                if depth == 0 {
                    return i;
                }
            }
            _ => i += 1,
        }
    }
    bytes.len()
}

/// The end of a literal closed by `quote`, from the byte after its opening
/// quote, with backslash escapes.
fn quoted_end(bytes: &[u8], from: usize, quote: u8) -> usize {
    let mut i = from;
    while let Some(&byte) = bytes.get(i) {
        match byte {
            b'\\' => i += 2,
            _ if byte == quote => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// A character literal or a lifetime at the `'` at `at`.
fn quote(text: &str, at: usize) -> (Kind, usize) {
    let bytes = text.as_bytes();
    match rest(text, at + 1).chars().next() {
        Some('\\') => (Kind::Literal, quoted_end(bytes, at + 1, b'\'')),
        Some(c) => {
            let after = at + 1 + c.len_utf8();
            if bytes.get(after) == Some(&b'\'') {
                (Kind::Literal, after + 1)
            } else if is_ident_start(c) {
                (Kind::Lifetime, ident_end(text, at + 1))
            } else {
                (Kind::Punct, at + 1)
            }
        }
        None => (Kind::Punct, at + 1),
    }
}

/// The identifier at `at`, or the literal it prefixes: a raw string
/// (`r"…"`, `br#"…"#`, `cr"…"`), a byte or C string, a byte literal, or a
/// raw identifier, whose text is the identifier alone.
fn word(text: &str, at: usize) -> (Kind, usize, usize) {
    let bytes = text.as_bytes();
    let end = ident_end(text, at);
    let prefix = text.get(at..end).unwrap_or_default();
    match (prefix, bytes.get(end).copied()) {
        ("r" | "br" | "cr", Some(b'#' | b'"')) => {
            let hashes = bytes
                .get(end..)
                .unwrap_or_default()
                .iter()
                .take_while(|&&byte| byte == b'#')
                .count();
            let open = end + hashes;
            if bytes.get(open) == Some(&b'"') {
                let mut closing = vec![b'"'];
                closing.resize(hashes + 1, b'#');
                let close = find(bytes, open + 1, &closing)
                    .map_or(text.len(), |offset| offset + closing.len());
                return (Kind::Literal, at, close);
            }
            let raw_ident =
                prefix == "r" && hashes == 1 && rest(text, open).starts_with(is_ident_start);
            if raw_ident {
                return (Kind::Ident, open, ident_end(text, open));
            }
            (Kind::Ident, at, end)
        }
        ("b" | "c", Some(b'"')) => (Kind::Literal, at, quoted_end(bytes, end + 1, b'"')),
        ("b", Some(b'\'')) => (Kind::Literal, at, quote(text, end).1),
        _ => (Kind::Ident, at, end),
    }
}

/// For each opening bracket, the index of its closing one, or the last
/// token's when it has none; other tokens map to themselves.
pub(super) fn closers(tokens: &[Token<'_>]) -> Vec<usize> {
    let mut close: Vec<usize> = (0..tokens.len()).collect();
    let mut open = Vec::new();
    let last = tokens.len().saturating_sub(1);
    for (index, token) in tokens.iter().enumerate() {
        if ["(", "[", "{"].iter().any(|bracket| token.is(bracket)) {
            open.push(index);
            if let Some(closer) = close.get_mut(index) {
                *closer = last;
            }
        } else if [")", "]", "}"].iter().any(|bracket| token.is(bracket))
            && let Some(closer) = open.pop().and_then(|opener| close.get_mut(opener))
        {
            *closer = index;
        }
    }
    close
}
