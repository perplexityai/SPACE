# Type-Driven Design

The principles from `principles.md` cash out, in practice, as a small set of recurring Rust patterns. This file is the reference for those patterns. The `.agents/plugins/pplx-space/skills/space-rust-development/references/types.md` recipe is the short version of this file you should follow while writing code.

---

## Newtypes (smart constructors)

**When.** A domain concept is currently passed as `String`, `u64`, `Uuid`, `Duration`, or `Option<…>`-of-the-same. Examples in SPACE: sandbox id, template name, guest path, idempotency key, memory size, process user name, content digest, build id, service name, socket path.

**Why.** Functions cannot accept the wrong noun by accident. Validation happens once, at the constructor, not at every call site (no shotgun parsing). `Debug`/`Display`/`Serialize` get domain-aware impls. Future invariants (length limits, charset rules, normalization) have a single place to live.

**Shape.**

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "&str", into = "String")]
pub struct SandboxId(String);

impl SandboxId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseSandboxIdError> {
        let raw = raw.into();
        if !is_valid_sandbox_id(&raw) {
            return Err(ParseSandboxIdError::Invalid);
        }
        Ok(Self(raw))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl FromStr for SandboxId {
    type Err = ParseSandboxIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::parse(s) }
}

impl TryFrom<&str> for SandboxId {
    type Error = ParseSandboxIdError;
    fn try_from(s: &str) -> Result<Self, Self::Error> { Self::parse(s) }
}

impl From<SandboxId> for String { fn from(v: SandboxId) -> String { v.0 } }
impl AsRef<str> for SandboxId { fn as_ref(&self) -> &str { &self.0 } }
```

**Rules.**

- Implement `FromStr`, `TryFrom<&str>`, and `serde::Deserialize`. If you find yourself writing a private `parse_optional_foo(Option<String>) -> Option<Foo>` helper, that is a smell — the type should `impl FromStr` and the call site should use `.map(str::parse).transpose()`.
- Implement `From<NewType> for String` (or the inner type) so re-serializing onto a wire is free.
- Don't implement `Deref<Target=str>`. It papers over the distinction the newtype was created to enforce.
- Prefer immutability: `pub struct SandboxId(String);` not `pub struct SandboxId { pub inner: String }`.
- Share each newtype from the crate that owns its concept, and extend it there instead of duplicating it. Don't fork parallel types per crate; that fragments the vocabulary and forces lossy conversions at every boundary. A new shared type goes in its owning crate (a protocol, value-type or service crate) or a new single-responsibility crate, not in `common-core`/`common-svc`.

---

## Sum types over flag bags

Whenever a struct has fields whose validity depends on the value of another field, the right answer is almost always to lift the discriminator into an enum and move the conditional fields into the variants that actually need them. See `principles.md` §2 for the canonical Connecting/Connected/Disconnected example.

**Heuristic.** Count the `Option<T>` fields on a struct. If two or more are "Some iff `state == X`", you have a sum-type-shaped thing pretending to be a product.

**Unknown wire values.** Reject unknown variants at the boundary by default.
If a protocol needs forward compatibility, model an explicit `Other(String)` or
`Other(serde_json::Value)` variant and define its handling. Do not use
`#[serde(other)] Unknown` to silently discard the unrecognized value.

---

## Typestate

**When.** An object has *operations that are only valid in some states*, and you want the compiler to refuse the wrong-order calls. Examples: a sandbox handle that supports `pause()` only when running, a connection that supports `send()` only after authentication, a builder that requires certain calls before `build()`.

**Shape (minimal).**

```rust
pub struct Sandbox<S> {
    id: SandboxId,
    _state: PhantomData<S>,
    inner: SandboxInner, // shared fields
}

pub struct Running;
pub struct Paused;
pub struct Stopped;

impl Sandbox<Running> {
    pub fn pause(self) -> Sandbox<Paused>  { Sandbox { _state: PhantomData, ..self } }
    pub fn stop(self)  -> Sandbox<Stopped> { Sandbox { _state: PhantomData, ..self } }
    pub fn exec(&self, cmd: Command) -> Result<Output, ExecError> { ... }
}

impl Sandbox<Paused> {
    pub fn resume(self) -> Sandbox<Running> { Sandbox { _state: PhantomData, ..self } }
    pub fn stop(self)   -> Sandbox<Stopped> { Sandbox { _state: PhantomData, ..self } }
    // No exec() — calling exec on a paused sandbox does not compile.
}

impl Sandbox<Stopped> {
    pub fn start(self)  -> Sandbox<Running> { Sandbox { _state: PhantomData, ..self } }
    // No pause/resume/exec.
}
```

Key encoding choices:

- **Transitions consume `self`.** `fn pause(self) -> Sandbox<Paused>`, not `fn pause(&mut self)`. The previous state handle disappears.
- **State markers are zero-sized.** `pub struct Running;` — no runtime cost.
- **`PhantomData<S>`** for the state parameter so the variance/`Send`/`Sync` story is clean.
- **Shared fields live in the outer struct.** Per-state data lives in a per-state struct that is `match`ed or stored inside the marker.

When to *not* use typestate:

- If the set of states is dynamic or unknown at compile time (use an enum and runtime checks).
- If clients legitimately need to hold a handle whose state may have changed under them (use an enum + transition methods that return `Result`).

**Sources:** http://cliffle.com/blog/rust-typestate/, https://hoverbear.org/blog/rust-state-machine-pattern/

---

## State machines with explicit transitions

When the typestate pattern is too rigid (because the runtime needs to inspect the state, persist it, serialize it), use an `enum` and gate transitions through methods that consume the value:

```rust
pub enum SandboxState {
    Waiting { since: Instant },
    Running { started_at: Instant, pid: ProcessId },
    Stopped { exit_code: i32 },
}

impl SandboxState {
    pub fn start(self, pid: ProcessId) -> Result<Self, IllegalTransition> {
        match self {
            SandboxState::Waiting { .. } => Ok(SandboxState::Running { started_at: Instant::now(), pid }),
            other => Err(IllegalTransition { from: other.kind(), to: StateKind::Running }),
        }
    }
}
```

**Rules:**

- Transitions consume `self` and return `Result<Self, IllegalTransition>` (or panic only in tests).
- There is exactly **one** `SandboxState` enum, shared across the API and the node manager. Never a `lifecycle: String` shadow in another crate — that is the bug we keep finding in code review.
- The transition matrix is enumerated in tests: a 2-D table of `(from, event) → expected outcome` makes regressions immediately visible.

---

## Common-trait hygiene

From the Rust API Guidelines (C-COMMON-TRAITS): types should eagerly implement the standard traits when they are sound. Specifically:

- `Debug` — always, for every public type. Without it, error messages and logs lose information.
- `Clone`, `Copy` — when cheap. `Copy` only for genuinely value-shaped data (≤ 16 bytes-ish, no `Drop`).
- `PartialEq`, `Eq`, `Hash` — when the type is used as a map key or compared for equality.
- `Default` — when there is an obvious zero value. Don't add `Default` just because deriving is easy; an `Address::default()` is rarely useful.
- `Display` — when the type has a single canonical user-facing string form. Newtypes wrapping ids/names usually should; complex structs usually should not.

Use the canonical conversion traits:

- `From<A> for B` for infallible conversions (B is a *superset* of A).
- `TryFrom<A> for B` for fallible ones; reach for this before inventing a `from_a_with_validation` constructor.
- `AsRef<U>`, `AsMut<U>` for cheap reference conversions.
- `FromStr` for "parse from text"; `From<String>` only when the conversion truly cannot fail.

For raw text conversion APIs, `impl AsRef<str>` or `impl Into<String>` can let the caller choose the cheap path. Domain APIs take the domain type; a generic string argument must not erase its invariant.

---

## Privacy and visibility

- Make fields private by default. A public struct with all-public fields is a frozen layout; future invariants cannot be added without breaking callers.
- In SPACE today, internal enums and structs are exhaustive — the compile error at every match site when a variant is added is what makes an enum stronger than a string. Add `#[non_exhaustive]` only on types in crates that ship externally (we have none yet).
- Provide constructors and accessors instead of public fields. `fn new(...) -> Self`, `fn id(&self) -> &SandboxId`.

---

## Closing checklist (the type-driven 90-second review)

Before sending the PR, scan the diff and ask:

- [ ] Every `&str`/`String` parameter at an inter-module function boundary: would a newtype from the crate that owns the concept say what this is?
- [ ] Every `Option<T>` field: is it `Some` iff some other field is `Some`/has a particular value? If yes, lift to an enum.
- [ ] Every `String` constant or `match` over string literals (`"start" | "stop" | …`): replace with an enum (`#[serde(rename_all=…)]` if it's serialized).
- [ ] Every `Vec<T>` or `HashMap<K,V>` returned from a function that the caller will treat as nonempty / unique-keyed: encode that in the type (`NonEmpty<T>`, an index-typed set).
- [ ] Every public type has `Debug`. Enums stay exhaustive unless the crate is part of a published SDK.
- [ ] Every fallible function: returns a small, named, `thiserror`-derived enum, not `String` or `Box<dyn Error>` (see `error-handling.md`).
