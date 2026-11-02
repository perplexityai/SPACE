# Error Handling

A function's error type is part of its API. Choose it on purpose.

The four-axis decision for every fallible function:

1. **Who handles this error?** The caller (programmatic), an operator (logs/alerts), or a user (HTTP response)?
2. **Can the caller act differently** based on which variant fires? If yes → enum. If no → opaque type with `Display`.
3. **Is this reusable Rust code or terminal orchestration?** Reusable code → typed enum, always. Terminal application code that reports the error and exits → `anyhow` is fine.
4. **Will the set of failure modes grow?** Almost certainly yes. While SPACE has no external SDK, leave the enum exhaustive (compile-error-on-add is the point). When a crate goes public, add `#[non_exhaustive]` then.

---

## The default: per-operation `thiserror` enums

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CreateSandboxError {
    #[error("template not found: {0}")]
    TemplateNotFound(TemplateName),

    #[error("quota exceeded for tenant {tenant}: {used}/{limit}")]
    QuotaExceeded { tenant: TenantId, used: u32, limit: u32 },

    #[error("scheduler refused placement: {reason}")]
    Placement { reason: PlacementRefusal },

    #[error(transparent)]
    Storage(#[from] StorageError),
}
```

**Why this shape.**

- Each variant tells the caller a *different thing they can do*. If two variants would be handled identically, fold them.
- `#[error("...")]` on each variant means `Display` is meaningful in logs without a custom impl.
- `#[from]` lets `?` lift sub-errors automatically, without flattening their internal structure into this enum (the boxed-source pattern is below).
- `#[non_exhaustive]` belongs on error enums **only when the crate is part of a stable public API.** SPACE's first stability boundary is proto, not cross-crate Rust — leave our enums exhaustive so adding `RegionUnavailable` produces a compile error at every match site.

**Anti-pattern: the crate-level kitchen sink.**

```rust
// DO NOT DO THIS
pub enum SbxMgrError {
    Io(io::Error),
    Db(sqlx::Error),
    Parse(ParseIntError),
    Network(reqwest::Error),
    NotFound(String),
    BadRequest(String),
    Internal(String),
    // ... 30 more
}
```

Problems:

- Exposes every dependency's error as part of *your* public API. A semver bump in `reqwest` becomes a semver bump in you.
- Sets every internal representation in stone — you cannot box a large variant later without a break.
- Forces every caller to match against variants they cannot possibly produce. The `parse_line` caller has to handle `Network`.

**Fix.** Split into per-operation errors (`CreateSandboxError`, `LoadStateError`, `ReconcileError`). If two operations genuinely share the same failure modes, *then* share an error. Not before.

> Source: Sabrina Jewson, *Modular Errors in Rust*, https://sabrinajewson.org/blog/errors.

---

## When you do want an opaque, future-proof error: the `std::io::Error` shape

For *vocabulary* errors that cross many module boundaries and must remain stable for years, mimic `std::io::Error`:

```rust
pub struct StorageError {
    repr: Repr,
}

enum Repr {
    Simple(StorageErrorKind),
    SimpleMessage(StorageErrorKind, &'static &'static str),
    Custom(Box<Custom>),
}

struct Custom {
    kind: StorageErrorKind,
    error: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageErrorKind {
    NotFound,
    PermissionDenied,
    AlreadyExists,
    Conflict,
    Throttled,
    Internal,
}

impl StorageError {
    pub fn new(kind: StorageErrorKind, e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self { ... }
    pub fn kind(&self) -> StorageErrorKind { ... }
}
```

Why this is the *right* design for a foundational error type:

- **Inspectable.** Callers `match e.kind()` for programmatic decisions. The kinds are stable; the structure is not.
- **Future-proof.** New kinds can be added with `#[non_exhaustive]`; the layout of `Custom` is private.
- **Cheap simple cases.** `Repr::Simple` and `Repr::SimpleMessage` avoid heap allocation for the common case (no underlying source).
- **Lossless.** The original source is preserved as `Box<dyn Error>` for richer reporting (`Display` chain, downcasting if you really need it).

Reach for this shape only for foundational, repo-wide errors. Most error types should be plain `thiserror` enums.

> Source: matklad, *Study of std::io::Error*, https://matklad.github.io/2020/10/15/study-of-std-io-error.html.

---

## `anyhow` vs typed errors

| Context                                                   | Use                       |
| --------------------------------------------------------- | ------------------------- |
| Terminal `main.rs` / CLI glue that reports and exits      | `anyhow::Result<T>`       |
| Reusable code in `crates/libs/**` and the `l10`–`l40` layer directories | typed `thiserror` enum |
| Service/reconciler/worker code in `crates/apps/**` and `crates/l50_app/**` | typed `thiserror` enum |
| Tests                                                     | `anyhow` or `eyre` is fine; clarity > rigor |

Why the asymmetry. In terminal orchestration, the caller is the OS, which only cares about exit code + log line. Everywhere else, the caller is *us*, three months from now, trying to handle `Throttled` differently from `NotFound`. `anyhow` erases that distinction. The crate's location does not decide this: a controller or background worker under `crates/apps/**` is reusable code when its caller decides whether to retry, clean up, map a status, or continue.

---

## `?`, `From`, and conversion

- Use `#[from]` (thiserror) for sub-error variants that should be liftable by `?` with no information loss and whose conversion is unambiguous.
- Use `#[source]` when wrapping a causal error with operation-specific context or when selecting the variant requires an explicit `map_err`.
- Use `#[error(transparent)]` only when intentionally exposing the wrapped error unchanged.
- Implement `From<SubError> for ParentError` only when the conversion is unambiguous. If converting loses information, write an explicit `.map_err(|e| ParentError::Foo { source: e, context: ctx })?` instead.

---

## Panics vs errors

| Situation                                                            | Choose       |
| -------------------------------------------------------------------- | ------------ |
| Invariant violated that our own code is responsible for upholding    | `panic!` / `unreachable!()` |
| Input from outside the process (network, file, env)                  | `Result`     |
| Programming error during development that we want loud feedback on   | `debug_assert!` |
| "Should never happen" but the input came from another service        | `Result` with an `Internal` variant — never trust other services |

Rule of thumb: a `panic!` is a contract violation by the *caller* (us). An `Err` is a runtime fact from *outside*. The two are not interchangeable, and changing one to the other is an API change.

---

## Hard rules for SPACE Rust code

These are restatements of things `AGENTS.md` already says, collected here so error-handling is in one place:

- **Never** return `Result<_, String>` from production code. Strings cannot be matched on; they convey nothing the caller can act on. (`#[allow(dead_code)]` is also not allowed; delete code rather than tagging it.)
- **Never** parse an error message to decide what happened. If you find yourself writing `if e.to_string().contains("foo")`, the error type is wrong.
- **Always** derive `Debug` on error types. The default `Display` from `thiserror` is fine; do not hand-roll it unless you have a reason.
- **Don't** mark error enums `#[non_exhaustive]` while SPACE has no public Rust API. Crates ship together; the first stability boundary is proto. Treating cross-crate boundaries as public induces operational cost (`_` arms, version bumps, doc churn) for no benefit. Add `#[non_exhaustive]` to error types only when a crate is published externally.
- **Always** include enough context. `ParseError::Range` is too thin; `ParseError::Range { line: usize, raw: String }` is what you want when you read the log six months from now.
- **Prefer enum errors over strings; prefer typed enums over `Box<dyn Error>`** at API boundaries. (Repeated from `AGENTS.md`.)

---

## Closing checklist (the error-design 60-second review)

- [ ] Does each reusable fallible function have a small, named error type that its callers can name through the same module boundary? Re-export a cfg-gated error alongside its function instead of exposing a private submodule path.
- [ ] Is the enum `#[derive(Debug, thiserror::Error)]`? (No `#[non_exhaustive]` on internal errors — first stability boundary is proto, not cross-crate Rust.)
- [ ] Does each variant carry the *context* a human reader (in logs) and a programmatic caller (matching) actually needs?
- [ ] Are sub-errors lifted via `#[from]` rather than flattened into local variants?
- [ ] At a *library* boundary: no `anyhow`, no `Box<dyn Error>` (unless you deliberately picked the io::Error pattern above), no `String`.
- [ ] Are panics confined to "our invariant, our fault" cases?
