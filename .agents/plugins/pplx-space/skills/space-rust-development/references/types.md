
# Type-driven design at the SPACE Rust boundary

You are writing Rust in `perplexityai/SPACE`. The reviewer will reject string-typed domain nouns and flag bags. This reference is the short recipe; the *why* lives in `docs/rust/principles.md`, `docs/rust/type-driven-design.md`, and `docs/rust/api-and-naming.md` — read those before designing a new public type.

## When to read this reference

- A function signature contains `&str`, `String`, `Option<String>`, `u64`, `Uuid`, or `Duration` for something that represents a domain noun.
- A struct has more than one `Option<T>` field whose Some-ness depends on the value of another field.
- A `match` is being written over string literals (`"start" | "stop" | …`).
- A field stores a "kind" or "state" as a `String`.
- A wire boundary (HTTP handler, gRPC service impl, CLI arg, env var) is being added or changed.

If none of those apply, you do not need this reference.

## The recipe (do these in order)

### 1. Identify the domain noun

For every wire-boundary parameter, ask: what does this value *mean*? If the answer is a sentence ("the id of a sandbox", "the action to take on the sandbox", "the path inside the guest"), the value needs a type — not `String`.

### 2. Reuse, don't fork

Before defining a new type, search the workspace for the crate that owns the concept:

- the value-type crates — `pplx-space-byte-size` (`MemorySize`, `DiskSize`), `pplx-space-semver`, `pplx-space-durations`, `pplx-space-content-digest`, `pplx-space-cpu-arch`, `pplx-space-k8s-ids`, `pplx-space-cluster-id`, `pplx-space-unix-socket-path`
- the protocol crates, for wire types (for example `pplx-space-sbx-mgr-protocol`, `pplx-space-node-mgr-protocol`, `pplx-space-net-gateway-protocol`)
- the service's own crates, for its domain types

Reuse the existing noun, imported from the crate that owns it.

If the existing type is *almost* right but needs a field or constructor, **extend it in the crate that defines it** (with tests). Do not fork a parallel local type next to your handler. Forking fragments the vocabulary and forces lossy conversions at every boundary.

### 3. If genuinely new, define the newtype canonically

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "&str", into = "String")]
pub struct FooId(String);

impl FooId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ParseFooIdError> {
        let raw = raw.into();
        if !is_valid_foo_id(&raw) {
            return Err(ParseFooIdError::Invalid);
        }
        Ok(Self(raw))
    }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl FromStr for FooId {
    type Err = ParseFooIdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::parse(s) }
}

impl TryFrom<&str> for FooId {
    type Error = ParseFooIdError;
    fn try_from(s: &str) -> Result<Self, Self::Error> { Self::parse(s) }
}

impl From<FooId> for String { fn from(v: FooId) -> String { v.0 } }
impl AsRef<str> for FooId { fn as_ref(&self) -> &str { &self.0 } }
```

Rules:

- Implement `FromStr`, `TryFrom<&str>`, `Serialize`/`Deserialize` via `try_from`/`into`. This is what makes serde, query parsing, env, and CLI all parse-don't-validate at the boundary.
- Do **not** implement `Deref<Target = str>`. That re-erases the distinction.
- Put a type a second crate could plausibly need in the existing crate that owns its concept; if none fits, create a small single-responsibility crate named for its responsibility (archcheck's `crate_names` rule bans `common`, `shared`, `util(s)`, `helper(s)`, `misc` and `core`). Do not add it to `common-core` or `common-svc`.

### 4. Strings → enums

If a field's value is "one of":

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxAction { Start, Stop, Pause, Resume, Suspend, Restore }
```

SPACE has no stable public Rust API today — first stability boundary is proto. Keep our internal enums exhaustive (no `#[non_exhaustive]`); the compile error on every match site when a variant is added is the point of choosing an enum over a string. (See `docs/rust/api-and-naming.md` for the future public-API exception.)

Reject unknown wire values strictly. Do **not** fold unknown variants into a catch-all:

```rust
// Bad — `Unknown` silently masks new server values, defeating the type.
#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SandboxLifecycleLoose {
    Created, Started, Stopped, Paused,
    #[serde(other)] Unknown,
}

// Good — serde fails the deserialize; the caller maps it to a domain error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxLifecycle {
    Created, Started, Stopped, Paused, Suspended, Restored, Deleted,
}
```

If a future use case genuinely needs a forward-compat escape hatch, add an explicit `Other(String)` or `Other(serde_json::Value)` variant at that point — never `#[serde(other)] Unknown` as a silent default.

### 5. Flag bags → sum types

If you wrote a struct like:

```rust
struct ConnectionInfo {
    state: ConnectionState,
    last_ping_time: Option<Instant>,
    last_ping_id: Option<u32>,
    session_id: Option<String>,
    when_initiated: Option<Instant>,
    when_disconnected: Option<Instant>,
}
```

…stop. Lift the discriminant and move the conditional fields into the variants:

```rust
struct Connecting { when_initiated: Instant }
struct Connected  { last_ping: Option<(Instant, u32)>, session_id: SessionId }
struct Disconnected { when_disconnected: Instant }

enum ConnectionState {
    Connecting(Connecting),
    Connected(Connected),
    Disconnected(Disconnected),
}
```

Heuristic: count the `Option<T>` fields on a struct. If two or more are "Some iff `state == X`", you have a sum-type-shaped thing pretending to be a product.

### 6. Lifecycle-style state machines: one type, transitions consume `self`

For sandbox/template/build lifecycle:

- Define **one** enum, in the crate that owns that lifecycle (e.g., `SandboxLifecycle`). Do not let a `lifecycle: String` shadow appear in any other crate.
- Transitions are methods on the enum that consume `self`:

```rust
impl SandboxLifecycle {
    pub fn start(self) -> Result<Self, IllegalTransition> {
        match self {
            SandboxLifecycle::Created | SandboxLifecycle::Stopped =>
                Ok(SandboxLifecycle::Started),
            other => Err(IllegalTransition { from: other, to: SandboxLifecycle::Started }),
        }
    }
}
```

- Cover the transition matrix in a unit test: a 2-D table of `(from, event)` → expected outcome.

When the typestate pattern fits (operations valid only in some states, statically), see `docs/rust/type-driven-design.md` § Typestate.

### 7. Common traits — derive eagerly

For every new type, default to:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
```

Adjust:

- Add `Copy` only for genuinely small (≤ 16 bytes) value types with no `Drop`.
- Add `Default` only when the zero value is meaningful.
- Add `Display` only when there is a canonical user-facing string form.
- Drop `Hash`/`Eq` if the type carries `f64` or `Instant` and equality is fuzzy.
- Drop `Serialize`/`Deserialize` if the type must not leak onto a wire.

`Debug` is non-negotiable on every public type.

## Self-review before opening the PR

Re-read your diff and check:

- [ ] No `&str`/`String` parameter at a function boundary represents a domain noun without a newtype.
- [ ] No `Option<T>` field is conditional on another field's value (lift to enum).
- [ ] No `match` over string literals (use an enum).
- [ ] No new "kind" or "state" stored as `String`.
- [ ] No parallel local type for a concept an existing crate already owns.
- [ ] Every public type has `Debug`.
- [ ] No `#[serde(other)] Unknown` fallback variants — unknown wire values fail deserialization rather than silently fold into a catch-all.
- [ ] Public struct fields are private; accessors are provided.

If any check fails, fix it before posting. The reviewer **will** ask, and the fix is cheap now and expensive once a downstream crate depends on the loose shape.

## When you need more

| You're doing this                       | Read                                             |
| --------------------------------------- | ------------------------------------------------ |
| Designing a multi-state object with operations valid only in some states | `docs/rust/type-driven-design.md` § Typestate    |
| Choosing common-trait derives and conversion traits | `docs/rust/api-and-naming.md`                    |
| Deciding when `#[non_exhaustive]` ever applies | `docs/rust/api-and-naming.md` § Flexibility (today: only for crates we ship as an SDK, which we don't) |
| Designing the error type to return     | [errors](errors.md)                               |
| Self-reviewing type discipline before PR | [review discipline](review-discipline.md) + `docs/rust/space-review-lessons.md` § 1, § 9 |

The principles backing this reference live in `docs/rust/principles.md`. If you find yourself fighting the rules above, re-read `parse-don't-validate` and `make-illegal-states-unrepresentable` (cited in `docs/rust/README.md`); the rules are downstream of those two ideas.
