# The Four Principles

Everything else in `docs/rust/` is a specialization of these four. If you have to choose between this file and any other rule you remember, choose these.

---

## 1. Parse, don't validate

> "If you ever find yourself writing a function that validates its input and then returns it unchanged, stop. You are about to lose information." — Alexis King, *Parse, don't validate*

**The rule.** A function that checks an invariant must encode that check in its **return type**, not just in a boolean it returns or a `panic!` it omits.

**The shape that is wrong:**

```rust
fn validate_sandbox_id(s: &str) -> bool { ... }

fn use_sandbox(id: &str) {
    if !validate_sandbox_id(id) { return; }
    // 50 lines later, the type system has forgotten the check
    do_thing(id);
}
```

**The shape that is right:**

```rust
fn parse_sandbox_id(s: &str) -> Result<SandboxId, ParseSandboxIdError> { ... }

fn use_sandbox(id: SandboxId) {
    // The argument *cannot* be an unvalidated string. Proven by the type.
    do_thing(id);
}
```

Concretely in SPACE: every domain noun at a wire boundary (HTTP query, JSON body, proto message, CLI arg, env var) is parsed **once** into the type its owning crate defines (`SandboxId`, `GuestPath`, `IdempotencyKey`, `TemplateName`, `MemorySize`, …) and travels as that type for the rest of the call graph. Strings reappear only when we re-serialize onto another wire.

The two failure modes this prevents:

1. **Shotgun parsing.** Repeated re-validation at every layer ("paranoid" code) — slow, easy to drift, and an indication that the type is wrong.
2. **Holes in the type system.** A function that returns `Option<T>` when the caller has *already* established the invariant forces the caller to handle a case they know is impossible. Use a stronger argument type instead (`NonEmpty<T>`, `SandboxId`, …) and make the impossible state unrepresentable.

**Source:** Alexis King, https://lexi-lambda.github.io/blog/2019/11/05/parse-don-t-validate/

---

## 2. Make illegal states unrepresentable

> "Model data representations such that nonsensical values are inexpressible." — Yaron Minsky, 2010.

**The rule.** Prefer sum types (`enum` with data-carrying variants) over flat product types whose fields contradict each other depending on a discriminant.

**The shape that is wrong:**

```rust
struct ConnectionInfo {
    state: ConnectionState,  // Connecting | Connected | Disconnected
    server: SocketAddr,
    last_ping_time: Option<Instant>,   // Some(_) only if Connected
    last_ping_id: Option<u32>,         // must be Some iff last_ping_time is Some
    session_id: Option<String>,        // Some(_) only if Connected
    when_initiated: Option<Instant>,   // Some(_) only if Connecting
    when_disconnected: Option<Instant>,// Some(_) only if Disconnected
}
```

There are roughly six implicit invariants in that struct, none of which the compiler checks.

**The shape that is right:**

```rust
struct Connecting { when_initiated: Instant }
struct Connected  { last_ping: Option<(Instant, u32)>, session_id: SessionId }
struct Disconnected { when_disconnected: Instant }

enum ConnectionState {
    Connecting(Connecting),
    Connected(Connected),
    Disconnected(Disconnected),
}

struct ConnectionInfo {
    server: SocketAddr,
    state: ConnectionState,
}
```

Now `last_ping_id without last_ping_time` is a compile error, not a code review comment.

**Applied to strings.** `action: String` where the value must be one of `"start" | "stop" | "pause"` is the degenerate case of this rule. Replace with:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxAction { Start, Stop, Pause }
```

If you genuinely need an escape hatch for an unrecognized value at a wire boundary (e.g., a newer client sent a value an older server doesn't know), add an explicit `Other(String)` or `Other(serde_json::Value)` variant — design it in deliberately, never `#[serde(other)] Unknown` as a silent default. In SPACE, the enum itself stays exhaustive; `#[non_exhaustive]` is for external SDKs we don't yet have.

**Source:** https://functional-architecture.org/make_illegal_states_unrepresentable/

---

## 3. Errors are part of the API; design them per-operation

> "Error types should be located near to their unit of fallibility." — Sabrina Jewson, *Modular Errors in Rust*

**The rule.** A function's error type tells the caller *what they can do about it*. A single kitchen-sink `Error` enum at crate level conflates failure modes the caller would handle differently. Prefer small per-operation error enums.

**The shape that is wrong:**

```rust
pub enum BlocksError {
    Io(io::Error),
    Parse(ParseIntError),
    Http(ureq::Error),
    InvalidRange,
    EmptyFile,
    // ...10 more
}

fn parse_line(s: &str) -> Result<Range, BlocksError> { ... }
fn fetch_latest()      -> Result<Blocks, BlocksError> { ... }
```

The caller of `parse_line` is forced to handle `Http` (impossible), and the caller of `fetch_latest` cannot distinguish "network" from "garbled bytes" without matching against variants that don't apply.

**The shape that is right:**

```rust
#[derive(Debug, thiserror::Error)]
pub enum ParseLineError {
    #[error("malformed range")]
    Range,
    #[error("invalid integer: {0}")]
    Int(#[from] ParseIntError),
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("http: {0}")]
    Http(#[from] ureq::Error),
    #[error("parse: {0}")]
    Parse(#[from] ParseLineError),
}
```

Three rules that follow from this:

1. **Use `thiserror` to derive error enums.** Never hand-write parsing of strings into errors. Never return `Result<_, String>` from non-test code in this repo.
2. **In SPACE, leave error enums exhaustive** (no `#[non_exhaustive]`) — every callsite gets a compile error when a variant is added, which is the point. `#[non_exhaustive]` is only for crates that ship as an external SDK; SPACE's first stability boundary is proto.
3. **The `std::io::Error` pattern is the gold standard for vocabulary errors:** an opaque struct with a small `Kind` enum exposed for programmatic handling and a boxed `source: Box<dyn Error + Send + Sync>` for the underlying cause. Reach for this shape when an error must be *both* inspectable and future-proof — typically only for crate-level errors that cross many boundaries.

**`anyhow` belongs in `main` and bin crates only.** Library crates in `crates/libs/` must return typed errors. `anyhow::Error` in a public lib signature erases the caller's ability to handle anything.

**Sources:**
- Sabrina Jewson, https://sabrinajewson.org/blog/errors
- matklad, https://matklad.github.io/2020/10/15/study-of-std-io-error.html

---

## 4. Async code never blocks the runtime

> "Async code should never spend a long time without reaching an `.await`." — Alice Ryhl

**The rule.** The Tokio runtime swaps tasks **only at `.await` points**. Anything between two awaits is, from the runtime's perspective, a single uninterruptible piece of work. A "long time" is roughly **>10–100 µs**.

What counts as blocking:

- File I/O via `std::fs` (use `tokio::fs` or `spawn_blocking`).
- CPU-bound work over ~100 µs (use `spawn_blocking` or `rayon`).
- `std::sync::Mutex::lock()` held across an `.await` (use `tokio::sync::Mutex`, or restructure to drop the guard before the await).
- `std::thread::sleep` (use `tokio::time::sleep`).
- Synchronous DB drivers (use the async equivalent).
- Heavy serde, regex, crypto on a hot path.

What to do instead:

- **CPU/blocking IO →** `tokio::task::spawn_blocking(move || { ... })`.
- **Shared state that must be held across an `.await` →** `tokio::sync::Mutex`, or even better, the **actor pattern** (see `async-and-tokio.md`): one task owns the state, others talk to it via `mpsc`.
- **Shutdown →** one `CancellationToken` per service, passed (cloned) to every spawned task. No bespoke `oneshot`/`broadcast`/`AtomicBool` shutdown channels.

**Sources:**
- Alice Ryhl, https://ryhl.io/blog/async-what-is-blocking/
- Alice Ryhl, https://ryhl.io/blog/actors-with-tokio/
- Tokio Graceful Shutdown, https://tokio.rs/tokio/topics/shutdown

---

## How these four interact

These principles compose. A typical SPACE handler should look like:

1. **At the wire boundary**, parse raw bytes into typed values (principle 1). Reject unknown enum variants or invalid newtypes here. Errors here are `BadRequest`-shaped and well-typed (principle 3).
2. **Inside the service**, hold *only typed* values; sum types make illegal combinations impossible (principle 2).
3. **Long-running work** is spawned, owns a `CancellationToken` child, never blocks the runtime, and exposes itself to other tasks via a handle that hides the channel (principle 4).
4. **Returning to the wire**, errors map to typed status codes / proto enums; the caller of our library can pattern-match on what went wrong, not parse a string.

Every rule in this directory is one of these four wearing different clothes.
