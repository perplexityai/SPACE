# API Design and Naming

A distillation of the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) keyed to the patterns we want for SPACE Rust crates. Treat this as a checklist; the upstream document has the full reasoning.

---

## Naming

- **Casing** follows RFC 430 (C-CASE): types and traits `UpperCamel`, values and functions `snake_case`, constants `SCREAMING_SNAKE`, lifetimes `'short_lowercase`.
- **Ad-hoc conversions** (C-CONV):
  - `as_X(&self) -> &X` — free, borrowed.
  - `to_X(&self) -> X` — expensive, possibly allocating.
  - `into_X(self) -> X` — consumes self.
  - Choose the prefix that matches the cost. Don't write `to_string` if it's just `&self.inner`.
- **Getters** (C-GETTER): field accessor is `fn field(&self) -> &T`, not `get_field`. The `get_` prefix is reserved for cases with multiple "gets" or fallible access (`HashMap::get`).
- **Iterators** (C-ITER, C-ITER-TY): `.iter()`, `.iter_mut()`, `.into_iter()`; the iterator types are named after the methods (`Iter<'a, T>`, `IterMut<'a, T>`, `IntoIter<T>`).
- **Word order** (C-WORD-ORDER) is consistent. If we have `for_each` and `try_for_each`, we do not also have `each_try`.

For SPACE-specific concepts: prefer the existing noun from the crate that owns the concept. If you find yourself debating `SbxId` vs `SandboxId` vs `SandboxIdentifier`, you are doing the wrong work — go look at what already exists in the workspace and use that name.

---

## Common traits (C-COMMON-TRAITS)

Eagerly implement the standard traits when sound:

`Copy`, `Clone`, `Eq`, `PartialEq`, `Ord`, `PartialOrd`, `Hash`, `Debug`, `Display`, `Default`, plus `serde::Serialize`/`Deserialize` for any type that crosses a wire boundary.

Conventions:

- **`Debug` is mandatory** for every public type. Missing `Debug` means lost information in logs and panics.
- **`Display` is opt-in** — only when the type has a single canonical user-facing string form.
- **`Default`** when there is an obvious zero value. Don't derive it reflexively; an `Address::default()` is rarely useful.
- **`Copy`** only for genuinely small (≤ 16 bytes-ish) value types with no `Drop`.
- **`Serialize` + `Deserialize`** belong on types that appear in wire formats. For internal types that we just don't want to leak into serialized form, omit them on purpose.

Use the canonical conversion traits:

- `From<A> for B` — infallible, B is a superset of A.
- `TryFrom<A> for B` — fallible. Reach for this before inventing a bespoke constructor name.
- `AsRef<T>`, `AsMut<T>` — cheap reference conversions.
- `FromStr` — "parse from text". Implementing this gives you free integration with `str::parse::<T>()`.

**Take generic arguments** in public APIs to be ergonomic:

```rust
// Good: caller chooses the cheap path.
fn open(path: impl AsRef<Path>) -> Result<File, io::Error>

// Worse: forces an allocation if the caller has a &str.
fn open(path: String) -> Result<File, io::Error>
```

---

## Predictability (C-METHOD-MAYBE-EXPENSIVE)

- A method that is cheap (`O(1)`, no syscall, no allocation) should be free of suffix.
- A method that *can* be expensive should signal it: `to_owned`, `clone`, `into_string`, `collect`.
- Methods named `is_*` / `has_*` return `bool` and are cheap. If you have to do I/O to answer, name it differently.

---

## Flexibility and future-proofing

SPACE has no stable public Rust API today. All crates ship together from the same workspace; the first stability boundary is **proto**, not Rust. So:

- **`#[non_exhaustive]` — not on internal enums or structs.** Keep our enums exhaustive so adding a variant produces a compile error at every match site. That cascading error is the point of choosing an enum over a string. If a crate ever ships as an external SDK, add `#[non_exhaustive]` to the public types in *that* crate at that time.
- **Private fields with public accessors** are still a good default when invariants need to be preserved (e.g. a `SandboxId` whose constructor validates), but don't reach for them on plain data types where the field IS the contract.
- **Builders for `> 3` parameters** or for any struct with optional fields. Don't expose a 7-argument `new`; expose `Foo::builder().a(...).b(...).build()`.
- **Type-safe builders** (typestate-flavored) when some fields are mandatory and some are optional: the `build()` method is only defined on the state where mandatory fields have been set.

---

## Type safety (C-NEWTYPE)

> "Wrap distinct kinds of values in different types, even if the underlying representation is the same."

This is the foundation rule of `type-driven-design.md`. The API Guidelines call it out independently because it is *the* technique for catching argument-swap bugs at compile time. `fn delete(sandbox: SandboxId, template: TemplateName)` cannot be called wrong; `fn delete(sandbox: String, template: String)` can.

---

## Documentation (C-CRATE-DOC, C-EXAMPLE)

We do **not** require a doc comment on every public item. The codebase is large, much of it is generated, and readable code beats decorative docstrings that go stale on the next refactor. Be strategic:

- **Document non-obvious behavior.** Latency contracts, retry budgets, idempotency guarantees, surprising side effects, ordering requirements. If the next reader has to grep retry sleeps to know whether a call can block for 30 seconds, write a doc comment.
- **`# Safety` section for every `unsafe fn`.** Non-negotiable — this is the unsafe contract.
- **`# Panics` only when a function can actually panic.** Don't add the section to advertise non-panicking behavior.
- **`# Errors` is welcome on functions whose error variants are part of the contract** (typically the ones whose error enum a caller will match on). Don't paste it on every `Result`-returning helper.
- **Doctests** earn their place on functions with subtle preconditions or non-trivial calling patterns. A doctest that just repeats the signature in prose is a tax.
- When (and only when) a crate ships externally, the documentation bar rises to "every public item." Until then, this file is the standard.

---

## Dependability

- **No panics in library code** unless a documented precondition is violated. `unwrap()`, `expect()`, indexing with `[i]`, integer arithmetic that can overflow — all reviewed for whether they are reachable.
- **`debug_assert!` for cheap internal invariants** during development; do not leave a hot-path `assert!` if the check is non-trivial in release.
- **`#[must_use]`** is targeted, not blanket. Apply it where ignoring the result is *genuinely dangerous*: types whose `Drop` performs a real side effect (transactions, RAII guards), builders whose unused `.build()` produces nothing useful, and futures returned from functions a caller can plausibly forget to `.await`. Don't decorate every `Result`-shaped wrapper; the standard library's `#[must_use]` on `Result` already covers the common case.

---

## Naming-vs-existence: when do we add a noun?

Before introducing a new struct/enum/alias for a domain concept (template reference, runtime kind, content digest, build id, service name, endpoint, socket path, auth context, transport error, etc.):

1. Search the workspace for the crate that owns the concept: the value-type crates (`pplx-space-byte-size`, `pplx-space-semver`, `pplx-space-durations`, `pplx-space-content-digest`, `pplx-space-cpu-arch`, `pplx-space-k8s-ids`, `pplx-space-cluster-id`), the protocol crates (for example `pplx-space-sbx-mgr-protocol`), and the service's own crates.
2. If the noun exists, reuse it, imported from the crate that owns it.
3. If the noun *almost* exists but lacks a field or constructor you need, **extend it in the crate that defines it** (with tests) rather than forking a parallel type next to your handler. Parallel types fragment vocabulary and force lossy conversions at every boundary.
4. If the concept is genuinely new and crate-local, define it locally. A new shared type goes in the existing crate that owns the concept: a wire type in that service's protocol crate, a generic value in the matching value-type crate, a service's domain type in that service's crate. If no crate fits, create a small single-responsibility crate named for its responsibility (archcheck's `crate_names` rule bans `common`, `shared`, `util(s)`, `helper(s)`, `misc` and `core`). Do not add new types or modules to `pplx-space-common-core` or `pplx-space-common-svc`.

---

## Reviewer's quick checklist

- [ ] `Debug` derived on every public type. `Display` only where canonical string form exists.
- [ ] Internal enums are **exhaustive** (no `#[non_exhaustive]`). Newtype wrappers carry their invariant in a validating constructor.
- [ ] Conversions use `From` / `TryFrom` / `AsRef` / `FromStr`, not bespoke names.
- [ ] Method names match Rust conventions (`as_*` / `to_*` / `into_*`, `iter` family, no `get_field` for plain getters).
- [ ] Non-obvious behavior is documented (latency, retries, idempotency, side effects). `unsafe` has a `# Safety` block.
- [ ] No new domain noun introduced if the crate that owns the concept already has one; a new shared type lives in its owning crate, not in `common-core` / `common-svc`.
- [ ] Generic arguments where reasonable (`impl AsRef<Path>`, `impl Into<String>`); not where they obscure the signature.
- [ ] `#[must_use]` applied where forgetting the result is genuinely dangerous — not as decoration.

> Source: https://rust-lang.github.io/api-guidelines/checklist.html
