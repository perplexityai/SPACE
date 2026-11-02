# Per-client activation rate

**Status: proposal, deferred.** This RFC specifies a per-client activation
rate: a token bucket per client, charged together with the cluster bucket.
It builds on
[Per-client admission independent of the substrate](2026-10-28-substrate-independent-client-admission.md),
called "the first RFC" below, which ships first. That RFC's terms,
attribution, key resolver, Precision rule and two admission sites apply here
unchanged.

## Terms

The first RFC's "Terms" section defines activation, admission, charge, token
bucket, client id and key. This RFC adds:

- **Client bucket.** A token bucket per key, alongside the shared cluster
  bucket. It bounds how fast one client activates, so one client can't take
  the whole cluster bucket or empty the warm pool.
- **Two-bucket charge.** One Redis script that checks the client bucket and
  the cluster bucket and takes a token from each, or from neither. Taking
  them in two steps would lose one bucket's token whenever the other refused.
- **Hold.** A token set aside without being spent yet, for a charge whose
  outcome isn't known when it has to be made. A hold is later *committed*
  (the token is spent) or *cancelled* (it's as if the request never ran).
  Holds replace refunds, which can over-credit a bucket (below).

## When to build it

The first RFC's "Phasing" section explains why the rate comes second: the
concurrency limiter already bounds a client's rate on the paths it gates, at
about `C / W` by Little's law, and the live cap bounds how many sandboxes a
client holds. What they leave open is fast activations on ungated paths, such
as warm-pool hits, taking a large share of the cluster bucket or the warm pool
before the cap stops them.

Build this if, with the first RFC enforced, the decision metrics show one
client taking a disproportionate share of cluster tokens or warm-pool hits
through paths the concurrency limiter doesn't gate, or show other clients
refused by the cluster bucket while one client stays under its cap. The
thresholds are a sizing decision this RFC doesn't make.

## Which bucket

The client bucket is named by the activation's resolved key:

| `ActivationClient` | Client bucket |
| --- | --- |
| `Client(i)`, or `Inherit` with record `i` | the key `i` resolves to |
| `Native`, or `Inherit` with record `native` | `native` |
| `Inherit` with record `NULL` | none: the cluster bucket alone |

`native` gets a bucket here, though it has no live cap, because the native
per-organization limit counts sandboxes, not how fast they start. Without a
bucket of their own, native callers could drain the cluster bucket.

## Charge points

The rule: every charged activation pays its client bucket once, at the same
point it pays the cluster bucket today, in one two-bucket charge. An exempt
activation stays exempt from both, evacuation included, so the rate can't
stall a node drain. There are three refinements.

**1. Coalesced activations pay once.** Where Kubernetes charges only the lease
winner, so does the client bucket. Paths that charge at entry once per request
would otherwise spend `N` client tokens for `N` concurrent requests to one
sandbox, on every burst, and refuse that client's own creates. Those are
implicit access (`charge_implicit_access_activation`, through
`connect_spaced_runtime_client` in `controllers/sandbox/spaced.rs`) and
Direct's entry charges, whose lease-race losers have already paid. That
violates the Precision rule. So those paths pay the client bucket once per
activation of a sandbox, not once per request. The two-bucket script sets a
per-sandbox marker when it charges the client, recording which client it
charged. While the marker exists, a request for that same client takes the
cluster token alone. A request for a different client pays its own bucket,
so concurrent requests from several clients each pay once, and the client
whose request ends up creating the runtime has always paid. The marker lives only while that activation is in progress: the
activation removes it when it ends, whether it succeeded or failed. Requests
that arrive after that find the sandbox running, which charges nothing, and
any later activation, a rebuild after a lost pod included, pays again. The
marker's expiry, thirty seconds after its last renewal by the running
activation, only backstops a crash. The cluster bucket
keeps today's per-request charge.

**2. Warm hits pay the client bucket, not the cluster's.** A warm hit spends no
cluster capacity, because its sandbox is already booted. But the pool is
shared. Without a client charge, one client could fill its whole cap from the
pool in a burst, or churn creates and deletes under its cap, and empty the
pool for everyone else, whose creates then go cold. So every create attempt
pays the client bucket before it branches between an admitted claim, a pool
bypass and a pool attempt. It doesn't yet know whether it will hit, so it
places a **hold**:

- A hit commits the hold once its adoption commits.
- A miss goes to the existing cold-create charge point, which takes the
  cluster token and commits the hold in one script.
- Any refusal or failure before then cancels the hold: the empty-cluster
  guard, the slot check, the cluster bucket, a `boot_mode=pool` miss, or a
  failed resource resolution. A hit whose adoption fails and falls back to a
  cold create keeps its hold for the cold path.

A dispatcher retry is a new attempt and pays again. An idempotent retry of
`POST /v1/sandboxes` returns the operation it already admitted and pays
nothing.

**3. Native admission-time claims hold at the route.** A native
`POST /v1/sandboxes` can claim a warm-pool slot inside its admission
transaction (`plan_admission_pool_claim`). That claim reserves pool capacity
before any attempt runs, so the route places the `native` hold there, after
the idempotency check confirms a new create and before the claim. The hold is
keyed by the operation and lives as long as the admitted claim does. The
attempt that consumes the claim uses that hold instead of placing its own, and
commits or cancels it as above. When the route doesn't commit an admitted
claim, for example when no slot was free, it cancels the hold, and the
dispatcher's attempt pays as an ordinary one. The payload's `pool_claim` field
alone isn't proof of payment, because it stays set after the claim is
released: an attempt that finds the claim released or the hold expired pays
as an ordinary one.

## Why a hold and not a refund

A bucket refills as `min(burst, tokens + elapsed × rate)`. So a token taken and
later handed back isn't the same as one never taken: the refill that tops up a
charged bucket is refill a never-charged one would have clamped away. With a
burst of two, a charge leaves 1, a refill brings it to 2, another request
takes one, and a refund leaves 2, where never charging would have left 1.
Under a saturated cluster bucket, cold creates are refused one after another,
each refund over-credits, and the client exceeds its rate for as long as the
saturation lasts. That repeats, so the Precision rule rejects it.

A bucket with holds keeps two levels and a set of live holds, each keyed by its
attempt with an expiry:

- `committed`: the tokens if every live hold commits. Admission reads this
  level: a request is admitted while `committed` is at least one, and a new
  hold takes one from it.
- `cancelled`: the tokens if every live hold is cancelled.

Both refill at the bucket's rate and clamp at its burst, as the single level
does today.

| Operation | Effect |
| --- | --- |
| Hold | `committed -= 1` |
| Commit | `cancelled = max(cancelled − 1, committed)`; `committed` unchanged |
| Cancel, with `h` live holds before it | `committed = max(committed, cancelled − (h − 1))`; `cancelled` unchanged |
| Expiry | treated as a cancel |
| No live holds left | `cancelled = committed` |

**Invariants.**
- `committed ≤ cancelled ≤ committed + h`, and the two are equal when no hold
  is live.
- `committed` follows exactly the path that charging at entry would have left,
  so a commit is exactly an entry charge, and a long-lived hold never stops the
  bucket refilling.
- A cancel never over-credits. With one live hold it lands exactly on the path
  where the attempt never ran. With several, the exact value lies between
  `cancelled − (h − 1)` and `committed + 1`, because charging `k` tokens leaves
  a bucket between zero and `k` below the uncharged one. The rule takes the
  lower end, so the error is under one token, toward refusing, and only for a
  client with overlapping holds.
- An expired hold, after a crash or a Redis error, pays nothing: an error of
  one toward the client, and only on a failure.

The holds, the markers and the client bucket share the client key's hash tag
(below), so every script touches one Redis slot.

## Composition with the first RFC

- **A create attempt** checks the live cap, then places its client hold (`429`
  if the client bucket is empty), both before it branches. A client at its cap
  or out of rate gets the `429` it can act on even when the cluster is also
  full.
- **Every other activation**, at its charge point, runs the empty-cluster guard
  and the slot check, then the two-bucket charge. The first RFC reserves the
  live cap before these wherever a new runtime is certain, so a cap refusal
  spends no token there.
- **Same slot in a clustered Redis.** Redis Cluster hashes a key with a `{...}`
  by the part inside the braces only. The existing cluster key,
  `space:cluster_admission:sandbox_activation`, has no braces, so it hashes as
  the whole string. A client key written as
  `{space:cluster_admission:sandbox_activation}:client:<key>` hashes that same
  string, so it lands in the same slot, and the existing key keeps its name.
  Renaming the cluster key would split the bucket between old and new images
  during a rollout, roughly doubling the admitted rate.
- **Refusals.** `ClientRateLimited` joins PR #1061's `ActivationRejectionKind`.
  It's backpressure, like `RateLimited`, because a bucket refills with time, and
  it passes through fallbacks like every other admission refusal (first RFC,
  Decision 6).
- **Interface.** `ActivationAdmission::admit` gains the resolved key as an
  `Option`, where `None` means no client charge.

## Configuration

Env defaults, where unset means no per-client rate, plus per-client overrides
in the quotas flag's `rate` field, keyed by client id. The resolver reads that
field only while an env per-client rate is set (first RFC, Decision 2), so a
flag push can tune a rate but can't turn one on, and it keeps the last valid
values through a flag outage. `unknown` and `native` have their own buckets.
The script charges whichever buckets are enabled, so the client bucket works
with the cluster limiter off, and a node-count probe error fails open only the
cluster bucket. Everything ships off.

## Rollout

The rate ships off, after the first RFC's attribution and live cap. On Direct,
don't enable it until the first RFC's Direct attribution pull request has
shipped and every worker returns a route-identity receipt. Until then Direct
records only at create, so a later inheriting wake would charge the previous
client's bucket.

## Open issues

These are left for whoever builds it:

- **Every exit of the native route ends its hold.** A Redis hold can't join the
  Postgres admission transaction. After the claim, the route can also end in an
  idempotency result, an admission or quota denial
  (`admit_within_sandbox_quota`), or a commit error, and each has to cancel the
  hold. A missed exit costs only the hold's expiry, during which the client's
  bucket admits one fewer request.
- **Inline native creates.** A native create with an admitted claim can run in
  the API process (`routes/public/sandboxes/inline_create.rs`), so the client,
  the claim and the hold have to reach that driver too.
- **Direct's charges.** Moving Direct's entry charges onto the two-bucket charge
  changes Direct's behavior, so it belongs in a Direct pull request with the
  `direct.rs` module doc updated, as the substrate `AGENTS.md` requires.

## Not covered

- Rate sizing, and the evidence thresholds that trigger building this.
- A per-class (create versus restore) split. The class stays a metric tag.
