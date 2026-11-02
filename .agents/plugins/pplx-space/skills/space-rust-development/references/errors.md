
# Error-type design at the SPACE Rust boundary

The `space-rust-development` entrypoint requires `thiserror`-derived error
enums at reusable Rust boundaries; this reference is the recipe. The *why*
lives in `docs/rust/error-handling.md` and `docs/rust/principles.md` § 3.

## When to read this reference

- A library function (in `crates/libs/**` or an `l10`–`l40` layer directory) returns `Result<_, _>`.
- A function returns `Result<_, String>`. (Almost always wrong — fix it.)
- A library crate signature has `anyhow::Result<_>` or `Box<dyn std::error::Error>`. (Always wrong in a public lib signature.)
- A crate has a single `Error` enum that mixes I/O, network, parse, and business errors. (Kitchen sink — split it.)
- A hand-rolled `impl Display` / `impl std::error::Error` is being written. (Use `thiserror`.)

## The recipe

### 1. Decide who handles this error

Ask:

- **Programmatic caller** — needs to `match` on what went wrong to recover? → Small named enum with meaningful variants.
- **Operator / log reader** — needs a message and a chain of causes? → Same: small enum, `thiserror` derives `Display`.
- **HTTP / gRPC response** — error must map to a status code? → Implement `From<MyError> for ApiResponse` at the handler boundary; *don't* leak HTTP details into the inner error.

If two variants would be handled identically by every caller, fold them. If you cannot name what each variant means in one short phrase, you have not understood the failure modes yet.

### 2. Use `thiserror`, always

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CreateSandboxError {
    #[error("template not found: {0}")]
    TemplateNotFound(TemplateName),

    #[error("quota exceeded for tenant {tenant}: {used}/{limit}")]
    QuotaExceeded { tenant: TenantId, used: u32, limit: u32 },

    #[error("scheduler refused placement")]
    Placement(#[source] PlacementRefusal),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
```

Rules:

- `#[derive(Debug, thiserror::Error)]` — always.
- `#[non_exhaustive]` — **not** on internal SPACE error enums (no public Rust SDK yet; first stability boundary is proto). Use it only when a crate ships externally. Until then, an exhaustive enum is the contract — a new variant produces a compile error at every callsite, which is exactly what we want.
- One `#[error("...")]` per variant. The `Display` impl is the log line; make it meaningful.
- `#[from]` for sub-errors that lift losslessly via `?`. Don't use `#[from]` if the conversion is ambiguous (two variants could plausibly carry the same sub-error).
- `#[source]` for sub-errors that are informational but not the primary discriminator.
- `#[error(transparent)]` when the variant *is* the sub-error and adding a wrapper message would be noise.

### 3. Per-operation, not per-crate

**Don't** define `SbxMgrError` with 30 variants spanning I/O, DB, network, and business logic. That:

- Forces every caller to handle variants they cannot produce.
- Makes every dependency's error type part of *your* public API.
- Sets internal representation in stone — you cannot `Box` a large variant later without a break.

**Do** define `CreateSandboxError`, `LoadStateError`, `ReconcileError`. If two share failure modes, share an error *then*, not preemptively.

### 4. Never `Result<_, String>`

A string cannot be matched on. A string cannot tell the caller whether to retry. If you find yourself writing:

```rust
return Err(format!("foo failed: {}", e));
```

…stop. Define an enum with a variant for "foo failed" and carry the underlying error as a field. The five extra lines pay for themselves the first time something tries to retry on `Throttled` only.

### 5. `anyhow` vs typed errors

| Where                                       | Use                       |
| ------------------------------------------- | ------------------------- |
| An app's `main.rs`, binary glue             | `anyhow::Result<T>`       |
| Library public signatures (`crates/libs/**`, `l10`–`l40`) | typed `thiserror` enum |
| Throwaway helpers private to one library file | typed if reused, `anyhow` only if truly local |
| Tests, `examples/`, build scripts           | `anyhow` / `eyre` fine    |

A typed enum in `main.rs` is fine; `anyhow` in a library's public signature is not. The asymmetry exists because in `main`, the caller is the OS; in a library, the caller is us in three months trying to handle one variant differently from another.

### 6. Foundational vocabulary errors — the `io::Error` shape

For errors that cross many modules and must remain stable for years (e.g., a storage-layer error used by every service): mimic `std::io::Error`.

```rust
pub struct StorageError { repr: Repr }

enum Repr {
    Simple(StorageErrorKind),
    Custom(Box<Custom>),
}

struct Custom {
    kind: StorageErrorKind,
    error: Box<dyn std::error::Error + Send + Sync>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageErrorKind {
    NotFound, PermissionDenied, AlreadyExists, Conflict, Throttled, Internal,
}

impl StorageError {
    pub fn new(kind: StorageErrorKind,
               source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self { ... }
    pub fn kind(&self) -> StorageErrorKind { ... }
}
```

Why this shape: callers `match e.kind()` (stable, inspectable); the struct layout is private (future-proof); simple-case constructors avoid heap allocation. Reach for it *only* for genuinely foundational errors — most error types are plain enums.

> Reference: `docs/rust/error-handling.md` § "The std::io::Error shape".

### 7. Panic vs error — pick on purpose

| Situation                                                           | Choose         |
| ------------------------------------------------------------------- | -------------- |
| Invariant our own code is responsible for upholding is violated     | `panic!` / `unreachable!()` |
| Input from outside the process (network, file, env, user)           | `Result`       |
| Development-time invariant we want to assert cheaply                | `debug_assert!` |
| "Should never happen" but the value came from another service       | `Result` with an `Internal` variant — don't trust other services |

A panic is a *contract violation by the caller* (us). An `Err` is a *runtime fact from outside*. Don't conflate.

In production code, every `.unwrap()` / `.expect()` is reviewed for whether it can fire. If it can, it must become a `?` returning a typed error.

### 8. Context, not just kind

```rust
#[error("malformed range")]
Range,                              // bad — log lines won't help debugging

#[error("malformed range on line {line}: {raw}")]
Range { line: usize, raw: String }, // good — actionable in logs
```

Carry the smallest values needed to understand the failure. Don't carry the entire request; do carry the id and the offending field.

## Self-review before opening the PR

- [ ] Every fallible function in a library crate returns a typed `thiserror` enum, never `Result<_, String>` and never `anyhow::Result` (except in `main`).
- [ ] Every error enum has `#[derive(Debug, thiserror::Error)]`. Internal enums are exhaustive (no `#[non_exhaustive]`); SDK-published enums get `#[non_exhaustive]` when we ship one.
- [ ] Each variant has a `#[error("...")]` whose message would help on-call at 3am.
- [ ] Sub-errors lift via `#[from]` (lossless) or are wrapped with `#[source]` + context — not flattened.
- [ ] No kitchen-sink `Error` enum that mixes unrelated failure modes.
- [ ] No `Box<dyn Error>` at a library boundary (unless deliberately following the `io::Error` shape).
- [ ] Every `.unwrap()` / `.expect()` in production code is justified (invariant we own, not external input).
- [ ] Panic messages, if any, name the invariant being asserted, not just "this can't happen".

## When you need more

| You're doing this                                | Read                                             |
| ------------------------------------------------ | ------------------------------------------------ |
| Designing a brand-new foundational error (cross-crate, multi-year lifetime) | `docs/rust/error-handling.md` § io::Error shape |
| Deciding between `#[from]`, `#[source]`, `transparent` | `docs/rust/error-handling.md` § `?`, From, conversion |
| Modeling a state that includes "failed" as a real state, not just an error | [types](types.md)                                |
| Wiring an error to an HTTP/gRPC response       | The handler layer's local error→status mapping; do not bake response shape into the inner error |
| Self-reviewing error-handling discipline       | [review discipline](review-discipline.md) + `docs/rust/space-review-lessons.md` § 2, § 3, § 4 |
