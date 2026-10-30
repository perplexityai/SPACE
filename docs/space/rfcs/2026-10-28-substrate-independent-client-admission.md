# Per-client admission independent of the substrate

**Status: proposal.** This RFC defines how SPACE limits what one client can
do to the shared cluster, in a way that works on both substrates
(Kubernetes and Direct). It decides who each sandbox activation is
attributed to, where that's recorded, how a client's live sandboxes are
counted, and how the per-client limits combine with the cluster limits.

It also reviews the open work toward that goal and says what to keep, rework
or drop:

- [PR #1058](https://github.com/perplexityai/SPACE/pull/1058) tags limiter
  decisions and reconnect metrics with a bounded client id.
- [PR #1061](https://github.com/perplexityai/SPACE/pull/1061) refuses an
  activation with a `503` when no eligible node has a free network slot.
- [PR #1066](https://github.com/perplexityai/SPACE/pull/1066) adds the
  nullable `sandboxes.activating_client_id` column (migration
  `20261030070237_sandbox_activating_client_id`). It first added the column
  to `sandbox_k8s_executions`, and was reworked to this RFC.
- [PR #1068](https://github.com/perplexityai/SPACE/pull/1068) carries the
  activating client into admission, records it per execution generation and
  labels pods with it.

A per-client activation rate is specified separately, in
[Per-client activation rate](2026-10-29-per-client-activation-rate.md), and
is built only if evidence shows this RFC leaves a gap (see "Phasing").

This RFC states each mechanism's rules and the invariants it has to keep.
Call-site detail belongs in the implementation pull requests and their
tests, which check against those invariants.

## Terms

- **Sandbox, runtime.** A sandbox is the durable object a caller creates. Its
  *runtime* is what actually runs it: a pod on Kubernetes, a Firecracker VM
  on a Direct worker. A suspended sandbox has no runtime, and a running one
  has exactly one.
- **Activation.** Any action that gives a sandbox a new runtime: a create, a
  start of a stopped sandbox, a restore of a suspended one, or a rebuild of a
  runtime that was lost. A reconnect to a sandbox that's already running
  isn't an activation, because it costs the cluster nothing new.
- **Admission.** The decision, made before an activation starts, whether to
  let it run. Admission runs a series of *gates*. Each gate passes or refuses
  with an HTTP status (`429` or `503`).
- **Token bucket.** A rate limiter. A bucket holds up to `burst` tokens and
  refills at `rate` tokens per second. Each admitted request takes one token,
  and an empty bucket refuses. Over time a bucket admits at most `rate` per
  second, with bursts of up to `burst`.
- **Charge.** To *charge* an activation is to take a token for it from a
  bucket at admission. Charging is how SPACE meters activations: only
  activations are charged, a refusal at a charge happens before any work
  starts, and a token is never handed back. The place in the code where an
  activation is charged is its *charge point*. An activation no bucket
  charges is *exempt*, such as an evacuation during a node drain.
- **Cluster bucket.** The one token bucket every activation shares
  (`cluster_admission/`). It protects the cluster from activation storms, and
  today it's the only bucket an activation is charged to.
- **Concurrency limiter.** The existing per-client limit on requests in
  flight (`client_concurrency/`). A request takes a *slot* when it starts and
  frees it when it returns. It doesn't count sandboxes: the sandbox a request
  created keeps running after its slot is free.
- **Live cap.** New in this RFC: the most sandboxes with a live runtime that
  one client may hold at once. A sandbox stops counting when its runtime
  ends.
- **Client id.** What the caller sent in the `X-Client-ID` header, kept when
  it's a valid Kubernetes label value and not a reserved name. A missing,
  invalid or reserved header becomes the id `unknown`, so every ECW
  activation has an id. Ids are unbounded: a caller can send anything.
- **Key.** What a client id *resolves to* for limits and metrics: the id
  itself when the operator lists it, `unknown` otherwise. Keys are bounded by
  configuration, so every bucket, cap and metric series uses a key, never a
  raw id. `native` is a reserved key for non-ECW callers.
- **Key set.** The ids the operator lists: the env allowlist plus the ids in
  the quotas flag.
- **Record.** The new `sandboxes.activating_client_id` column: the client id
  whose activation created the sandbox's current runtime.
- **Pod label.** `space.pplx.net/client-id` on a sandbox pod: the client id
  whose activation created that pod.
- **Reflector.** A watch-backed, in-memory copy of Kubernetes objects. Every
  API replica keeps one of all sandbox pods (`K8sCaches`), so reading pods
  costs no apiserver call.
- **Pending set.** New in this RFC: a short-lived Redis record of admissions
  whose pods the reflectors may not have seen yet (Decision 5).
- **Warm hit, miss.** A create *hits* the warm pool when it adopts a
  pre-booted pool sandbox, and *misses* when it has to boot one (a cold
  create).
- **Lease winner, follower.** On Kubernetes, concurrent restores of one
  sandbox coalesce: one request wins a lease and does the work, and the
  others, the *followers*, wait for it. Only the winner is charged.

## Decisions requested

1. **Who's charged.** Every activation carries an `ActivationClient`: an ECW
   client id, *inherit* (act for the sandbox's recorded client) or *native*.
   Each activation resolves to one client id, which its live cap, its pod
   label and the record all use.
2. **Ids are stored, keys are resolved at use.** Records and labels store
   ids. One resolver turns an id into a key against the current key set, so
   a configuration change takes effect everywhere at once.
3. **The record follows the runtime.** Whoever creates the runtime writes
   the record, with the same value as the pod label: the row inserts,
   Kubernetes pod tracking, and Direct's start and restore receipts.
4. **The pod label is immutable.** It's set when a pod is created. The one
   exception is a warm-pool pod, which is labeled synchronously at adoption,
   before the adoption commits.
5. **One count interface.** `Substrate::live_sandboxes_for_client` counts a
   client's live sandboxes from the reflector, and the Redis pending set
   covers admissions the reflector hasn't seen. Direct answers "unknown", and
   admission admits.
6. **Composition.** The live cap runs before any token is spent wherever a
   new runtime is certain, and where a pod is about to be created otherwise.
   A running sandbox is never refused.
7. **What ships.** Decisions 1 to 6, PR #1061's slot check, enforcement of
   the existing concurrency limiter, and a concurrency slot for ECW resume.
   The per-client rate waits for evidence.

## Precision

These limits are guardrails, not accounting. An error of about one sandbox
or one token, in either direction, is acceptable when it's rare, temporary
and corrects itself:

- admitting one sandbox over a cap, or one activation over a rate;
- refusing with `429` when there's room for one more.

An error that repeats on every request of some kind, accumulates, or never
corrects isn't acceptable: a client whose sandboxes stop counting for good,
a bucket that a routine path drains, a cap that a routine path fills. Each
mechanism below is judged against this rule, and the rule settles races
this RFC doesn't analyze one by one.

## Phasing: why the per-client rate is separate

Three per-client limits are possible:

- the **concurrency limiter** bounds requests in flight
  (`SPACE_API_CLIENT_CONCURRENCY_MAX_PER_CLIENT`, 32 by default) on ECW
  create, connect and fork;
- the **live cap** bounds sandboxes held;
- a **per-client rate** would bound how fast a client activates.

**Little's law** says that in a steady state, the requests in flight equal
the arrival rate times the time each one takes. So a client held to `C`
requests in flight, each taking `W` seconds, completes at most `C / W` per
second: the concurrency limiter is already an indirect rate limit. With
`C = 32`, a 5-second restore allows about 6 per second, and a 0.1-second
warm hit about 320 per second (illustrative durations). The bound is
loosest exactly when activations are fast and cheap, and it applies only to
paths that take a slot:

- ECW resume, ingress wakes, and native file, process and control access
  take none. This RFC adds a slot to ECW resume, which has the same shape as
  connect.
- Native `/v1` and admin calls run as persisted operations in the
  dispatcher, or, for a native create that won an admission-time pool claim,
  in a task the request waits on briefly
  (`routes/public/sandboxes/inline_create.rs`). Neither takes a slot.
- A caller that disconnects frees its slot while its restore keeps running.

The live cap and an enforced concurrency limiter bound the failure this
design is shaped around: one client's bulk reconnects waking many suspended
sandboxes at once. The cap bounds how many it holds, the limiter bounds how
fast it gets there, and the cluster bucket and slot check still protect the
cluster. What's left open is fast activations on ungated paths taking a
large share of the cluster bucket or the warm pool. The rate RFC closes that
if metrics show it happening. Its machinery (charging warm hits, holding
tokens across a pool attempt, a native bucket) is the most complex of the
three limits, so it waits.

## Background

### Where activations are charged today

Admission goes through the `ActivationAdmission` trait. Each substrate call
that may activate carries a `RecoveryCharge` (`recovery_charge.rs`), which
says whether that call charges. The charge points:

- **Cold create:** after the warm-pool attempt misses, before the row insert
  (`controllers/sandbox/queries.rs`). A warm hit isn't charged.
- **Explicit lifecycle calls, at entry:** `SandboxAction::activation_class`
  (`controllers/sandbox/lifecycle.rs`) charges Start and Wake of a `created`
  or `stopped` sandbox as cold, and Start, Wake and Restore of a `suspended`
  one as a restore. On Kubernetes, those of a `suspended` or `restoring`
  sandbox skip the entry charge and defer it to the lease winner (below). A
  Restore of any other lifecycle isn't charged.
- **Implicit activation on access, at entry:**
  `charge_implicit_access_activation` charges an access (ingress,
  `spaced_client` routes) that has to wake the sandbox first, once per
  request.
- **Deferred, inside the substrate:** on Kubernetes, a restore of a
  `suspended` or `restoring` sandbox charges the lease winner
  (`charge_restore_claim`), and paths that rebuild a lost pod charge a
  deferred cold activation (`charge_cold_activation`).
- **Direct:** charges at entry, because it can't join another request's
  restore.
- **Exempt:** evacuation, Stop and Kill, and some live-lifecycle rebuilds
  pass `RecoveryCharge::Exempt`.

### What each substrate has

| Substrate | Runtime | Per-generation state | In-memory view |
| --- | --- | --- | --- |
| Kubernetes | sandbox and restore pods | `sandbox_k8s_executions`, one `active` row | the sandbox-pod reflector, in each API replica and the cluster controller |
| Direct | VMs on registered workers | none; route identity in Direct-owned tables | none; Postgres is the inventory |

The [substrate `AGENTS.md`](../../../crates/libs/api/src/controllers/substrate/AGENTS.md)
governs changes here: a new operation is a required `SubstrateAdapter`
method with an explicit Direct arm, shared code asks a capability rather
than which substrate runs, and a change to Direct behavior goes in its own
pull request.

## Why the in-flight attribution design changes

PR #1066 and PR #1068, as first written, recorded the client on each
`sandbox_k8s_executions` generation when it was inserted, and rendered the pod label from that row.
Three problems follow:

- **It's Kubernetes-only.** Direct has no generation rows.
- **Most generation inserts don't know the client.** Placement, the
  started-recovery handoff and evacuation run without one, so each insert
  copies the previous generation's value. That's inheritance logic spread
  over two crates.
- **A claim isn't an activation.** The value is written when a generation is
  claimed. A claim can be refused, fail, lose a race or be abandoned by a
  crash, and every exit would have to restore the old value. A crash reaches
  no exit at all.

Recording at the shared controller's finalization doesn't work either. It
also runs for reconnects and resident wakes, and its statements carry no
ownership fence, so a stale finalization could overwrite a newer
activation's record.

## Decision 1: three kinds of activation client

```rust
pub enum ActivationClient {
    /// An ECW activation, for this client id.
    Client(ClientId),
    /// Acting for the sandbox's recorded client: implicit access, reconcilers.
    Inherit,
    /// A native `/v1` or admin call.
    Native,
}
```

It replaces PR #1068's `Option<ClientKey>`, where `None` can't say whether
it means inherit or native. Each activation resolves to one client id,
which serves its cap, its pod's label and the record:

| `ActivationClient` | Client id | Live cap | Label and record |
| --- | --- | --- | --- |
| `Client(i)` | `i` | the key of `i` | `i` |
| `Inherit`, record `i` | `i` | the key of `i` | `i`, normally unchanged |
| `Inherit`, record `native` | `native` | none | `native` |
| `Inherit`, record `NULL` | none | none | no label; the record stays `NULL` |
| `Native` | `native` | none | `native` |

**Why inherit exists.** A sandbox an ECW client created is often woken by
something other than that client's own call: an ingress request, a native
file read, a reconciler's rebuild. Those wakes still give the client one
more live sandbox, so they count against its cap. Classifying them `Native`
would relabel the sandbox `native` and drop it from its client's cap for
good.

**Why native has no cap.** Native creates already have a per-organization
limit (`native_sandbox_limits.rs`), and a native live cap is a separate
decision. `native` is still stored, so an ingress wake of a native sandbox
stays native, and `NULL` keeps one meaning: nothing recorded, such as a row
from before the column.

**Accepted gap.** Native calls authorize by organization, so a native call
can activate a sandbox an ECW client holds, which then becomes `native` and
leaves that client's cap. An organization holding both credentials can
therefore hold sandboxes outside its ECW caps. The cluster limits still
apply, and the threat model (Decision 2) doesn't cover a caller choosing its
own attribution.

**Classification by intent.** `Client` and `Native` are for activations a
caller explicitly asked for. ECW create, connect, resume and fork are
`Client`, and native and admin creates and lifecycle calls are `Native`.
Anything else that happens to create a runtime is `Inherit`: implicit access
(ingress, `spaced_client` routes, opening a port, rollback, the spaced
control-wake fallback), a rebuild as a side effect of Stop or Kill,
reconcilers and evacuation, and the dispatcher's implicit command wake
(`is_implicit_command_wake`).

## Decision 2: ids are stored, keys are resolved at use

PR #1058 and PR #1068 each define a bounded client key over the same
allowlist. Keep two types and one resolver:

- `ClientId`, PR #1068's validated label-value type, is what's stored: in
  records, labels and persisted operations. The request boundary reads the
  header into a `ClientId` once. A missing or invalid header, or one naming
  a reserved key (`unknown`, `overflow` or `native`, in any case), becomes
  `unknown`. So an ECW caller that sends no header, as raw SDK callers do,
  is `Client(unknown)`: it's labeled, recorded and capped as `unknown`, and
  keeps working as before.
- `ClientKey` is what an id resolves to: the id when the key set names it,
  `unknown` when it doesn't, and always `native` for `native`. `native`,
  `unknown` and `overflow` are reserved, so no allowlist entry can claim
  them.
- The resolver lives with PR #1058's classification. The concurrency
  limiter, the cap and metrics all call it at the moment of use.

**Why limits use keys, not ids.** The id is whatever the caller put in the
header, so it can be anything. Using it directly for limits would break in
three ways:

1. **Caps would stop working.** If every id got its own bucket and cap, a
   misbehaving client could send a fresh id per request and get a fresh cap
   each time. Ids the operator doesn't know have to share one limit, and
   that shared limit is the `unknown` key.
2. **State would be unbounded.** Every bucket is a Redis key and every
   metric tag a time series. Keyed by raw id, callers could create as many
   of each as they liked. Keys are bounded by configuration: the listed ids
   plus `unknown` and `native`.
3. **Per-client settings need a name.** An operator gives a client its own
   cap or quota by listing it, and a listed id is exactly what becomes its
   own key.

**Why records and labels store ids, not keys.** The key set changes over
time. Say `batch-7` isn't listed today, so it resolves to `unknown`. If its
sandboxes stored `unknown`, listing `batch-7` tomorrow couldn't move them to
their own key, because the stored value no longer says who they belong to.
Storing `batch-7` and resolving it at each use makes a key-set change take
effect everywhere at once, existing sandboxes included, with nothing to
rewrite.

**How an id maps to a key.** In two steps:

1. **Header to id,** once, at the request boundary: a valid label value
   that isn't reserved is kept, and anything else becomes `unknown`. Native
   `/v1` and admin calls get `native`.
2. **Id to key,** at each use, against the current key set: `native` maps
   to `native`, an id the key set names maps to itself, and anything else
   maps to `unknown`.

With a key set of `{alpha, beta}`:

| `X-Client-ID` sent | Stored id | Key |
| --- | --- | --- |
| `alpha` | `alpha` | `alpha` |
| `gamma` | `gamma` | `unknown`, and `gamma` once it's listed |
| none | `unknown` | `unknown` |
| `Not A Label!` | `unknown` | `unknown` |
| `native` (spoofed) | `unknown` | `unknown` |
| a native `/v1` call | `native` | `native` |

The one exception is a degraded key set (below): while the quotas flag is
down and declared required, the live cap counts each unlisted id on its
own, capped at `unknown`'s value, rather than pooling them into `unknown`.

**The key set.** The env allowlist
(`SPACE_API_CLIENT_CONCURRENCY_KNOWN_CLIENTS`) plus the ids named in the
quotas flag. Today the flag has the concurrency `overrides` map. This RFC
adds a `live` field for per-client caps, and the rate RFC a `rate` field.
Each field decodes on its own, so a malformed one drops only itself, and
each accepts an object delivered as text, as `QuotaOverrides` already does.

**A flag tunes only what the environment enabled.** The flag client answers
`Override::Disabled` while the environment's switch is off
(`libs/feature-flags/src/policy.rs`). The resolver applies that per field:
it reads `overrides` while the limiter's mode isn't `off`, `live` while an
env default live cap is set, and `rate` while an env per-client rate is set.
A single shared switch wouldn't hold: the limiter's mode defaults to
`observe`, so a flag push could then turn on a cap the environment never
configured. To cap only some clients, an operator sets a high env default
and overrides below it.

**When the flag doesn't resolve.** The flag client can't tell a broken flag
from an absent one, so:

- The resolver keeps its last valid flag value, the key set and the cap
  values together, for up to `KEY_SET_STALENESS` (ten minutes). A short
  outage changes nothing.
- Past that, or before a process first resolves the flag, it uses the env
  allowlist alone.
- Whether a flag is expected is declared, not guessed: a new env setting,
  `SPACE_API_CLIENT_QUOTAS_FLAG_REQUIRED`, off by default. With it on, an
  env-only key set is *degraded*, and the resolver reports that through a
  metric and a warning.
- While degraded, the live cap counts each unlisted id on its own, against
  `unknown`'s cap value, and `unknown`'s own check counts only callers that
  sent no valid id. Pooling them would put an override-only client's whole
  fleet into `unknown`, whose cap would then refuse every `unknown` caller
  for as long as the outage lasted. Each unlisted id stays bounded by that
  cap, and the concurrency limiter and the cluster limits bound their total.
  When the flag resolves again, every sandbox counts under its own key, with
  nothing to correct.

**Behavior change.** Today any allowlisted id gets its own concurrency
bucket. With one resolver, an entry that isn't a valid label value is
ignored with a warning, and that caller shares the limiter's anonymous
bucket. Operators with such an entry rename it before upgrading.

**Threat model.** `X-Client-ID` is self-asserted, and the allowlist isn't a
secret. The per-client limits contain well-meaning clients that misbehave,
such as retry storms and runaway batch jobs. They don't stop a hostile
caller from sending another client's id. Authentication, the
per-organization quotas and the cluster limits do that.

## Decision 3: the record follows the runtime

Add `sandboxes.activating_client_id text NULL`: the client id whose
activation created the sandbox's current runtime. It's written with the
same value as the pod label, so the two agree by construction.

**Why record at runtime creation.** The record has one consumer: an
`Inherit` activation, which needs to know whose sandbox it's waking, to
charge that client and to label the new pod. Recording at creation gives the
record the label's definition: who last spent capacity on this sandbox.
Recording on success or at a claim would need a fence on every success,
failure and crash path (see "Alternatives considered").

**Writers.** There are three, and nothing else writes it:

1. **Row inserts.** `sandbox_store::insert_unplaced` (cold create and fork)
   and `sandbox_store::insert_pooled` (warm-pool adoption) write the
   create's id, or `native`. A native create that runs inline uses the
   dispatcher's create executor, so its `Native` reaches `insert_pooled` the
   same way.
2. **Kubernetes pod tracking.** The record follows the pod that the
   sandbox's active execution row tracks. Whenever a statement changes that
   row's `pod_uid` to a different pod, the same transaction sets the record
   to that pod's label. The label comes from the pod itself, through the
   create response or the reflector's view, never from the request. A
   different pod with no label sets the record to `NULL`, because nothing
   says who created it. An unlabeled view of the pod the row already tracks
   writes nothing. Its label can be missing only because an older image
   created it, or because the cached view predates the adoption patch, and
   neither means the record is wrong.
3. **Direct receipts.** Direct writes when it commits a successful node
   response that started or restored a VM: the owned restore receipt for a
   lease restore, and `persist_route_identity_receipt` for a charged start,
   or for a wake whose transition started or restored the VM. It writes only
   when the receipt's route-identity upsert is accepted, so it's exactly as
   fenced as the route identity it rides with. A worker from before route-identity
   receipts returns none, so its successful start leaves the previous
   record in place. On Direct the live count fails open (Decision 5), so
   only an inheriting wake's rate charge could follow that stale record, and
   the rate RFC's rollout keeps the rate off on Direct until every worker
   returns receipts.

**Pod-tracking cost.** The pod-tracking statements
(`update_active_execution_pod_metadata` and its callers, observer hot paths
included) lock only the execution row today, while multi-row mutations lock
`sandboxes` first. So the record write is conditional. Under the
execution-row lock, if the UID is unchanged and the record already matches
the label, nothing more happens (an unlabeled view matches any record for an
unchanged UID). Otherwise the transaction restarts in the established order,
`sandboxes` and then the execution row, re-checks, and writes. That slow
path runs about once per new pod. The read path's gate in
`pod_ops/read.rs` compares the record as well, so the next observer repairs
a record that lags.

**Guards.** The record write skips lifecycles in which no activation can be
creating a pod (`deleting`, `deleted`, `suspending`, `failed`,
`quarantined`, `cleanup_required`), while the UID write proceeds as today.
The statement lives in a new `attribute_runtime` module of
`pplx_space_sandbox_store`, which owns guarded transitions on `sandboxes`.
The Kubernetes caller holds the execution-row lock and re-checks the
generation and UID itself. That's a choice: `lost::mark_started_lost` does
check the execution row in its own statement, but keeping this one to
`sandboxes` lets the Direct receipts, which have no execution row, use the
same form.

**Invariants.**
- After any committed pod-tracking write, the record equals the tracked
  pod's label, or is `NULL` when that pod was created unlabeled.
- No observer of a pod that's already tracked moves the record away from
  that pod's creator.
- No claim, reconnect or resident wake ever writes the record.

### Carrying the client and the charge

The client reaches pod rendering and the Direct receipts in the activation's
`RecoveryCharge`. Today one `Exempt` value covers both "already paid
upstream" and "nobody pays", and nothing says where the live cap runs. So
the RFC splits it:

```rust
pub struct RecoveryCharge {
    state: ChargeState,
    cap: CapPolicy,
    client: ActivationClient, // the kind of activation; resolution doesn't change it
    key: KeyResolution,       // the one client id its cap and label use
    admission: Option<Arc<dyn ActivationAdmission>>, // `None` only in tests
}

pub enum ChargeState {
    Enforce, // charge here: this path owns the activation's admission
    Paid,    // charged upstream, and admitted
    Exempt,  // nobody charges it
}

pub enum CapPolicy {
    Reserved,       // reserved upstream, before any token was spent
    Check,          // checked in `ensure_pod_exists`, if a pod gets created
    ReserveAtClaim, // decided by the Kubernetes lease winner (Decision 6)
    Skip,           // not capped
}

pub enum KeyResolution {
    Resolved(Option<ClientId>), // resolved at entry; `None` means the record was `NULL`
    Unresolved,                 // a substrate-started rebuild reads the record itself
}
```

`Inherit` is resolved once, at entry, from the `sandboxes` row the action
already loaded. So the charge, the cap and the label use one value, even if
pod tracking moves the record in the meantime. Every `RecoveryCharge`
constructor takes an `ActivationClient`, and PR #1068's client-less
`RecoveryCharge::EXEMPT` constant goes away, so the compiler lists every
call site. Direct writes the record for a start only when the state is
`Enforce` or `Paid`.

**Shape.** No default and no backfill. Adding a nullable column without a
default is catalog-only. A format `CHECK` allows `NULL`, the reserved ids and
the Kubernetes label-value grammar (at most 63 characters), as the
data-store rules keep for fixed formats, so a write that bypasses the Rust
parsing fails instead of storing a value no label could carry. It's added
`NOT VALID`, so the migration doesn't scan `sandboxes`; it constrains every
new write, and the existing rows are all `NULL`. There's no index, because nothing filters
on the column. A change bumps `updated_at` through the existing trigger,
about once per new pod.

## Decision 4: an immutable pod label

`space.pplx.net/client-id` is rendered when a pod is created, from the
activation's client id, and never changes afterwards. So it answers one
fixed question: whose activation created this pod. A pod counts against that
client for as long as it lives. A restore-lease takeover that reuses a pod
an earlier attempt built leaves it counting against the earlier client.
That's correct, because that client's activation spent the capacity.

**Warm-pool adoption** is the one exception. A pool pod is built before it
has an adopter, and adoption is its activation, so adoption patches the
label in:

- The patch is synchronous, retried while throttled, and runs **before**
  the adoption commits (`commit_pooled_sandbox_claim`) and before the
  adopter's setup. PR #1068 sends it in the background, after the commit.
  Patching first means a slot that has to be given back is still a clean
  pool pod, and the label exists before the client can use the sandbox.
- It runs on both adoption paths: a pool attempt
  (`try_create_from_pool_with_substrate`) and a native admitted claim
  (`try_create_from_admitted_pool_claim_with_substrate`).
- It's a new required `SubstrateAdapter` method, `label_adopted_execution`,
  because slot invalidation is private to the Kubernetes pool module. Direct's
  arm returns success: it adopts node-side and has no pod to label.

**When the patch fails,** the adoption fails, not the create:

- The slot is invalidated, never returned, whatever the apiserver
  answered, and the pool's cleanup deletes the pod. A returned slot goes
  back to the front of the claim queue, so a refusal that repeats would
  send every later pool hit to the same failing slot, an error on every
  request that the Precision rule rejects. Invalidating costs one rebuilt
  warm pod, and a patch whose outcome is unknown can't land on a reused
  pod.
- A pool attempt then continues cold. A cold pod is labeled when it's
  created.
- A native admitted claim instead returns the release-pending error it
  already returns for a claim it declines, and the retry builds cold under
  its own id.
- A `boot_mode=pool` create has no cold path, so it fails with the existing
  `503`.

**Invariant.** No pod ever goes back to the pool carrying a client's label,
and no adopted pod stays unlabeled.

## Decision 5: one count interface

```rust
pub fn live_sandboxes_for_client(
    &self,
    ids: &CountedIds,
    activating: Option<SandboxId>,
    already_pending: &HashSet<SandboxId>,
) -> LiveSandboxCount;

/// Which label values one cap check counts. Admission builds it once per
/// check, from one resolver snapshot, and passes the same value to the
/// pending set.
pub enum CountedIds {
    Exactly(Vec<ClientId>),   // a listed key's id, or one unlisted id while degraded
    AllExcept(Vec<ClientId>), // `unknown`: every id except the key set and `native`;
                              // while degraded, `unknown` is `Exactly([unknown])`
}

pub enum LiveSandboxCount {
    Known(u64),
    Unknown(LiveSandboxCountUnknown),
}

pub enum LiveSandboxCountUnknown {
    Unsupported,  // no per-client inventory on this substrate
    PodCacheCold,  // the reflector hasn't finished its initial list
    PodCacheStale, // the reflector's watch has gone quiet for too long
}
```

`activating` is the sandbox being activated, which never counts against its
own activation, so reaching or replacing a sandbox the client already holds
is never refused. `already_pending` holds the sandboxes the pending set
already counts. The method is a synchronous cache read on `impl Substrate`,
dispatched per substrate like PR #1061's `network_slot_headroom`. It answers
`Unknown` rather than an error, and each `Unknown` reason gets its own
metric value, so a cap that never fires can be told apart from one that
can't see.

### Kubernetes

Count the distinct sandboxes, other than `activating`, that have a
non-terminal sandbox or restore pod whose label is one of the ids `ids`
selects. Pending and terminating pods count, because they hold scheduler
capacity until they're gone. Pods count rather than `sandboxes` rows,
because pods are what hold capacity.

Scanning the whole reflector on every check would put a scan on the warm
path. So the count keeps an index, updated from the reflector's watch
events: label value → sandbox → UIDs of that sandbox's qualifying pods. A
sandbox leaves a value's set only when its last qualifying pod does. Sets,
unlike a running counter, absorb repeated and reordered watch events, and
they're rebuilt from the cache after a relist. It's the pattern PR #1061
uses for `NodeNetworkSlotsState`. The label joins `SandboxPodView` and
`SandboxPodKey`, as the substrate rules require.

A reflector whose watch stalls after its initial list keeps serving its old
index, and its readiness signal stays green. Counting from it would let that
replica admit over the cap on every request once the pending entries
expire. So the reflector records when it last received a watch event or
bookmark, and a count from a reflector quiet for longer than
`POD_CACHE_STALENESS` answers `Unknown(PodCacheStale)` and fails open with
that reason, which the metric makes visible.

### The pending set

A pod becomes visible only when its watch event reaches a replica's
reflector. Until then the admission is invisible on every replica. Warm hits
make that window matter. A client holding `C` concurrency slots, with hits
of `W` seconds and a watch delay of `d`, can be admitted about `C × d / W`
times before any reflector shows it. With 32 slots, 0.1-second hits and a
0.5-second delay, that's 160 sandboxes over the cap. So admission keeps a
short-lived record of recent admissions in Redis, next to the existing
buckets.

**Rules.**
- There's one sorted set per client id, not per key, so a pending entry
  follows its id through key-set changes, as a label does. Each member is a
  sandbox id, scored by its expiry, `LIVE_PENDING_TTL` (thirty seconds)
  after its last renewal. The attempt that owns a member renews it at every
  step until its pod carries the label: until `create_sandbox_pod` returns
  on a cold path, or the adoption label patch returns on a warm hit. That
  covers each re-claim, swap and fallback, and it renews periodically while
  it waits. After that, the TTL only has to cover one watch delay. So a slow
  activation stays counted however long it takes to create its pod.
- A cap check takes three calls. `pending_live_members(ids)` reads the live
  members and a cutoff time from Redis. The substrate counts the visible
  sandboxes not among them. Then `reserve_live_slot` runs a Lua script that
  counts the pending members again at the same cutoff, and adds this
  activation's member only if visible plus pending is below the cap. If the
  activation's own sandbox is already pending, the script renews it and
  admits. For `AllExcept`, the script counts every registered id outside the
  exclusion, so ids registered after the read count too.
- Each member stores the set of attempts that own it, one owner token each,
  because two attempts for one sandbox can overlap and share its member. A
  reservation or renewal adds its token. `release_live_member` removes only
  the releasing attempt's token, and the member goes only when its last
  owner does, so a failed attempt never uncounts a sibling that's still
  creating the pod.
  `swap_live_member` moves a create's member to another sandbox id: to the
  adopted pool sandbox on a warm hit (right after the claim, before the
  label patch), between re-claims after a prepare failure, and back to the
  planned id on every fallback to a cold create.
- An attempt that ends without a live sandbox removes its own member, on
  every refusal and failure path. Delete, stop and suspend call
  `forget_live_sandbox(sandbox)` once the sandbox's pods are deleted. It
  removes the sandbox's member from every client's set, through a companion
  hash from each sandbox to the ids holding members for it, because two
  clients can each hold one.
- Every key shares one hash tag, `{space:live_pending}`, so a script can
  read every set in a clustered Redis. That puts the traffic on one Redis
  slot, as the cluster bucket's single key already does, at two or three
  small calls per activation.
- A Redis error admits, with its own metric reason, as the other limiters
  do.

**Keeping the read and the script consistent.** Three gaps need closing:
- *A member deleted between the read and the script* would count nowhere.
  So scripts delete an expired member only `LIVE_PENDING_GRACE` (sixty
  seconds) after its expiry, and a script whose cutoff is already older than
  the grace refuses to decide, so the caller redoes the check. Owner releases
  and teardown delete at once, but only members whose activation ended
  without a live pod, which is right to stop counting.
- *A member added between the read and the count* can be counted twice, as
  pending and as visible. So a script that would refuse, while counting
  members the read missed, returns without reserving, and the caller redoes
  the whole check: a new read, a new substrate count against it, and the
  script. It does that at most twice, then refuses. A refusal that survives
  needs new same-client members to land in every pass's gap, and errs by at
  most those.
- *The registry of ids that have members,* which an `unknown` check needs,
  must stay bounded. It's a sorted set scored by each id's latest member
  expiry, and scripts prune it by the same rule as members: an id leaves
  when its last member is released or forgotten, or has expired past the
  grace period. So it holds only ids admitted in about the last ninety
  seconds, however many ids a caller has sent over time. Ids in the key set are bounded by the key set. Every
  other id holds members counted against `unknown`'s cap, so those ids are
  bounded by that cap. With `unknown` uncapped, or while the key set is
  degraded, the bound is how fast ECW activations bring new ids, which an
  enforced concurrency limiter holds to `C / W` across all unlisted callers.

**Invariants.**
- An admitted activation counts toward its client from its admission until
  its pod is visible to the reflector, or until the activation ends.
- A sandbox with no live pod and no running activation counts for no one.
- The errors left come from the edges of the pending window, such as a
  member that expires before its pod becomes visible. Each is about one
  sandbox, briefly.

### Why the reflector plus a pending set

| Option | Problem |
| --- | --- |
| Count `sandboxes` rows in Postgres | A query on every activation attempt, counting lifecycle states rather than the pods that hold capacity |
| A Redis counter per client | Needs a decrement on every way a runtime ends, and drifts for good when one is missed |
| The reflector alone | Misses admissions within one watch delay: up to `C × d / W` sandboxes |
| A per-replica overlay of recent admissions | Each replica sees only its own admissions |
| The reflector plus a pending set | Each covers the other's blind spot. The pending set holds only recent admissions, each expiring on its own, and the reflector stays the truth for everything older |

### Direct

Direct has no in-memory inventory, so it answers `Unknown(Unsupported)`, and
the cap admits with that reason. A count from Postgres would be a separate
Direct pull request.

## Decision 6: how the limits compose

### Order

1. An ECW create, connect, fork or resume takes a concurrency slot first,
   and releases it when it returns.
2. **A create** checks the live cap before it branches between an admitted
   claim, a pool bypass and a pool attempt. So forks and pinned creates are
   capped too, and a warm pod isn't claimed only to be refused. A miss then
   runs the existing gates.
3. **Every activation**, at its charge point, runs the existing gates: the
   empty-cluster guard (`429`) and the cluster bucket (`429`). PR #1061's
   slot check (`503`) runs only for a cold create, one whose warm-pool
   attempt missed or that skips the pool, between those two. Every other
   activation can reuse an existing pod or a node its disk is pinned to, so
   a cluster-wide count of free slots would misjudge it. Those stay bounded
   by the cluster bucket and the live cap. The live cap runs before the
   gates, or at pod creation, according to its `CapPolicy`.

### Where the live cap runs

A client at its cap that keeps retrying must not drain the shared bucket.
So the cap is reserved **before** any token is spent wherever the lifecycle
guarantees a new runtime, and checked **at pod creation** where the
activation only might create one. Either way a running sandbox is never
refused: a call that finds a live pod creates none and isn't checked.

| `CapPolicy` | Paths |
| --- | --- |
| `Reserved` | A create; a Start or Wake of a `created` or `stopped` sandbox; on Direct, a Start, Wake or Restore of a `suspended` one; an implicit access to a `created`, `stopped` or `suspended` sandbox, once the auto-wake policy has allowed the wake |
| `ReserveAtClaim` | Kubernetes restores that defer their charge to the lease winner. A fresh claim from `suspended` reserves before its charge and becomes `Reserved`. A takeover of a lapsed `restoring` lease, which can reuse a pod, becomes `Check`, and so does a call that claims no lease because the row moved. Followers create no pod and skip it |
| `Check` | Paths where the sandbox should already be running and its pod may be gone: a Start, Wake or implicit wake of a live lifecycle; a Restore of anything but `suspended`; a SPACE `Resume`; a rollback; `control_runtime_client`; the access path for live lifecycles, which opening a port and reconnect also take |
| `Skip` | Stop, Kill, teardown, evacuation and reconcilers, with `Exempt`; a create's own follow-up start and runtime-client calls, with `Paid`, since the create already paid and reserved |

Refusing a `Skip` path would fail cleanup and node drains to enforce a
guardrail.

**The `Check` point.** `ensure_pod_exists`
(`kubernetes/pod_ops/ensure_pod.rs`) is the one point that decides whether a
pod gets created. So it gains the `RecoveryCharge` and runs the check on the
branch that's about to create a pod, and before the early cross-node redrive
of an interrupted source-pinned restore. A redrive that replaces a pod this
same activation just created isn't checked again, because the cap already
ran for that activation. On a refusal, `ensure_pod_exists` abandons any claim
it created (`abandon_created_unmaterialized_create_claim`) and returns a
distinct cap error. Its callers treat it as a refused charge, not a failed
activation: a lease restore releases its claim
(`release_rejected_restore_claim`), and a started recovery fails as a refused
charge would fail it.

**Deferred cold charges.** The access path charges `charge_cold_activation`
when it owns a started recovery or the lifecycle isn't dormant
(`kubernetes/execution.rs`), before `ensure_pod_exists` decides. Without a
guard, a client at its cap would pay a cluster token and be refused on every
retry after a node loss. So when the reflector shows no qualifying pod for
the sandbox, that path first runs a read-only pre-check of the same count,
and refuses before spending anything. When the pod is visible, it skips the
pre-check and reuses the pod.

**Accepted errors.** A lease takeover, or a race past the pre-check, can
spend one cluster token before the cap refuses. A follower waiting on a
winner that the cap refused gets the winner's refusal, though its own client
may be under its cap; its retry checks under its own client. Both are rare
and one-off.

### Refusals

- **Statuses.** A full cluster isn't the caller's doing, so it's a
  retryable `503`, as PR #1061 implements. A client at its own cap gets a
  `429` with the existing jittered `Retry-After` and a message naming the
  cap. A create checks its cap first, so a client at its cap gets the `429`
  it can act on even when the cluster is also full.
- **Retries.** PR #1061's `ActivationRejectionKind` tells the async
  dispatcher how to retry: `RateLimited` is backpressure, and `EmptyCluster`
  and `NoFreeSlot` retry against the attempt budget. This RFC adds
  `ClientAtLiveCap`, which retries against the budget too, because a cap
  lifts only when the client releases a sandbox, not with time.
- **Fallbacks pass every refusal through.** A path with a fallback must not
  turn an admission refusal into a retry under a weaker charge.
  `control_runtime_client` returns a `429` from its first, charged call, but
  any other error falls into an uncharged fallback wake, so PR #1061's `503`
  would rebuild the pod uncharged on a full cluster. It can't match on the
  refusal today: `runtime_client` returns a `ControllerError`, and the
  conversion turns `AdmissionRejected` into a plain rate-limited error
  (`controllers/substrate/mod.rs`). So `ControllerError` gains an admission
  marker that carries the `ActivationRejectionKind`, with an unchanged
  status, and the pass-through matches on that marker.
- **Ingress** maps controller errors to `unavailable`
  (`controllers/sandbox/ingress.rs`), so a cap refusal there shows as
  unavailable. Changing that is separate work.

### Two admission sites

The cluster controller's public-operation dispatcher
(`public_operation_dispatcher.rs`) also runs admission, for native `/v1`
operations, against the same Redis. Everything here applies there too:

- Each persisted operation carries its `ActivationClient` and resolved id in
  its payload, and the dispatcher doesn't infer the client from
  `api_product`. An ECW fork is enqueued with `ApiProduct::Native` but
  carries the forking client, so it's `Client`.
- `WakeSandbox` is classified when it's claimed, with
  `is_implicit_command_wake`, because a keyed `:wake` can take over a queued
  system wake without touching its payload. No actor is `Inherit`, and a
  caller is `Native`.
- Operations from before the payload field fall back: `WakeSandbox` as
  above, `ForkSandbox` as `Inherit` (charged to no client, record `NULL`),
  and everything else as `Native`.

### Configuration and independence

- **Caps.** Env defaults, where unset means no cap, plus per-client
  overrides in the quotas flag's `live` field, read only while an env
  default is set (Decision 2). `unknown` has its own cap, and `native` has
  none. Everything ships off.
- **Each gate runs whenever it's enabled.** Today
  `prepare_activation_admission` returns early when the cluster limiter is
  off, and PR #1061 puts its slot check after that return. Here the slot
  check and the live cap run on their own switches. The empty-cluster guard
  reads the cluster limiter's refill rate, so it runs only when the cluster
  limiter does. A node-count probe error fails open only the empty-cluster
  guard and the cluster bucket.
- **Fail-open.** A Redis error, a cold reflector, an unsupported substrate
  or an unconfigured cap admits, each with its own metric reason.

## Alternatives considered

- **Per-generation column (PR #1066's first version).** Kubernetes-only, needs
  inheritance in every generation insert, and records claims, not
  activations.
- **Writing the record when a claim is won, or on success.** Every exit that
  gives up a claim, every success statement and every crash would need a
  fence, and the record would drift from the label.
- **A mutable label that follows the latest activation.** It needs a patch
  on every pod reuse, synchronous to stay correct, and a reconciler for
  failed patches.
- **Keeping an ECW key through a native call.** It needs separate cap and
  label keys per activation, and closes only one of the paths out of an ECW
  cap.
- **Native callers under `unknown`.** It mixes two populations that already
  have different quotas.
- **Gating every path with the concurrency limiter instead of a rate.** Its
  bound still depends on latency, a slot per data-plane request counts
  reconnects that activate nothing, and dispatcher operations would need
  slots held across their execution.

## What to keep, rework or drop

| Work | Verdict | What changes |
| --- | --- | --- |
| PR #1058, client id on limiter and reconnect metrics | keep; merge first | It becomes the resolver's home: reserve `native`, ignore non-label entries with a warning, and derive the metric tag from `ClientKey`. Interning and `overflow` stay a metrics rule. |
| PR #1061, slot check with a `503` | keep, with two changes | Run the slot check whenever it's enabled, not only when the cluster limiter is. Pass its refusal through `control_runtime_client`'s fallback, using the `ControllerError` marker. |
| PR #1066, the attribution column | rework | Now `sandboxes.activating_client_id` (migration `20261030070237_sandbox_activating_client_id`), moved from `sandbox_k8s_executions` because the record follows the sandbox's runtime on both substrates, not a Kubernetes generation, and renamed because it holds an id, not a key. It keeps the nullable, no-default, no-backfill shape and the rollback note, and adds the `NOT VALID` format `CHECK`. |
| PR #1068, attribution and labels | rework | Keep the client in `RecoveryCharge`, the fork payload field, the pod label and the adoption patch, made synchronous and moved before the commit. Drop the generation-row writes, their inheritance logic, the change to `admit`'s signature and the fallback to `ClientKey::unknown`. Add `ActivationClient`, the `ChargeState` and `CapPolicy` split, the insert writes, the `attribute_runtime` store module with conditional pod-tracking writes, and the label in `SandboxPodView`. |
| Concurrency limiter enforcement | turn on | Set `SPACE_API_CLIENT_CONCURRENCY_MODE=enforce` with caps. Gate ECW resume with `acquire_client_slot`, which needs the header threaded into `SandboxController::action`. |
| Direct attribution | separate pull request | Write the record from the owned restore receipt, and from `persist_route_identity_receipt` for a charged start or a wake that started or restored the VM. Thread the client into `restore_execution_with_candidates`, and update the `direct.rs` module doc. |
| Per-client live cap | build to this RFC | `live_sandboxes_for_client`, the pending set and its scripts, the `CapPolicy` checks, and `ensure_pod_exists`'s new parameter. |
| Per-client activation rate | separate RFC, on evidence | See [Per-client activation rate](2026-10-29-per-client-activation-rate.md). |

## Rollout

1. PR #1058 and the `sandboxes` migration merge first. Older images don't
   name the column. The migration's brief `ACCESS EXCLUSIVE` lock on a busy
   table can lose to a long transaction under the one-second
   `lock_timeout`, so rely on the migrator's retry and run it outside peak
   load. Its `NOT VALID` `CHECK` is added under the same lock and scans
   nothing.
2. Attribution ships to the API and the cluster controller, and changes no
   admission decision. During a mixed rollout, old images write no record
   and create unlabeled pods. When a new-image observer starts tracking such
   a pod, it sets the record to `NULL`, so that sandbox counts against no
   one until a new-image activation with a client of its own (`Client` or
   `Native`) sets it. An inheriting activation keeps `NULL` (Decision 1). The live cap is still
   off, so nothing enforced depends on it. Forks queued before the payload
   field fall back to `Inherit`.
3. Enforce the concurrency limiter.
4. Enable the live cap once pods have turned over with labels.
5. The per-client rate, if evidence calls for it, follows its own RFC.

A sandbox that stays suspended through the rollout keeps a `NULL` record, so
its first inheriting wake pays only the cluster bucket. Nothing can backfill
it, because no history says who activated it.

Roll back in reverse order. The column's down migration drops it, so roll
back the images that use it first.

## Not covered

- Cap sizing.
- Moving ingress and Direct charges behind their lease claims.
- A live cap for native callers.
- A live count for Direct.
