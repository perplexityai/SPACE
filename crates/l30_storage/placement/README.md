# pplx-space-placement

This crate answers one question: **which sandbox rows in Postgres may change
placement state, and who is allowed to change them.** It does not create
pods, call nodes, or run reconcilers — that work belongs to its callers.
Think of it as the lock manager for sandbox placement: you check work out,
you do the work elsewhere, you check the result back in.

## Why isn't this just `create()` and `restore()`?

Because a restore is not one action — it is a claim, then about two minutes
of real work, then a completion, and the dangerous races all happen during
those two minutes. A single `restore()` call would have to either own the
pod-and-node work itself (pulling Kubernetes into this crate and turning it
into another reconciler) or pretend a multi-minute operation is one atomic
database write (which is the lie the legacy writers told, and where the
double-drive and resurrection bugs lived).

So the interface is the two phases, and the *intent* — create, user wake,
evacuation, pool adoption — is the `ClaimKind` you pass to the claim. The
whole trait — suspend, abandon, evacuation, node loss, quarantine, delete,
override — is declared today so callers compile against the full shape at
once, but only the user-restore and evacuation-restore claims and the
fenced completion are implemented: every other operation (and the
create/pooled claim kinds) returns a typed "not yet", and its signature
stays refinable until the PR that ports its legacy writers lands. No
caller routes through the crate yet — this PR is deliberately
implementation-only, so the guarded transactions are reviewable in
isolation; the api's user wake path cuts over in the stack's next PR, and
the drain's evacuation caller in its own operation PR after that. If you
want a `restore()` verb, it lives in the caller that composes the phases —
see the walkthrough below — because a user wake, a drain sweep, and a
repair loop each compose them differently.

## How a wake actually flows

1. **Claim.** `claim_placement` runs one guarded transaction: if the
   sandbox is claimable, it flips the row to `restoring` and hands you a
   fence (an execution generation plus a lease token). The fence is your
   proof of ownership, and the lease means ownership expires — a crashed
   caller blocks nobody for more than the lease window. If you lose the
   claim, the outcome tells you *why* and *what to do*: someone else owns
   it (retry after their deadline), it's throttled (retry at the given
   instant), it was superseded (drop it), or it isn't wakeable at all.
2. **Do the work.** You create the pod, drive the node restore, wait for
   readiness. The crate is not involved.
3. **Complete.** `complete_placement` is one CAS that re-checks everything at
   flip time: the proof you present (`PlacementProof::Fence` here) still owns
   the row, no teardown landed meanwhile, and the execution you're about
   to expose records routable evidence — `running`, with a pod IP, on a
   registered SPACE node (evidence as fresh as the substrate's last
   write, not a liveness probe). Only then does the row become
   `started`/`on_node`, with the node assignment stamped from the
   execution's own pod. If any check fails you get `CasLost`,
   nothing was written, and the answer is to surface it — never to retry
   with the same fence. A caller whose owning attempt vanished presents
   `PlacementProof::LapsedClaim` instead, and the same CAS verifies the
   lapse (lease pair present and expired) before adopting the row's own
   fence.

The compiled version of this walkthrough is
[`examples/user_wake.rs`](examples/user_wake.rs). It builds under
`cargo check --all-targets`, which CI enforces — so if the interface
changes and that file (or this description of it) goes stale, the build
breaks instead of the docs lying.

## The state model underneath

The vocabulary this crate is built around is four placement states, not
sixteen lifecycle strings:

| Placement | + artifact? | + observed runtime | Renders to customers as |
| --- | --- | --- | --- |
| `Unplaced` | none | — | created / stopped |
| `Unplaced` | snapshot | — | suspended |
| `Placing` = a live lease + an execution being materialized | maybe | — | restoring / starting |
| `Placed` | — | VM running | started |
| `Placed` | — | VM paused (pod alive) | paused |
| `Evacuating` | building | — | (internal to drain) |

Two rules make the table work. **Intent lives on the sandbox row,
cluster-authored** — delete requests, quarantine holds, claimability; it
must survive incarnations (a suspended sandbox has no execution to hang
intent on). **Evidence lives on the execution row, node-authored** — one
row per placement attempt: its progress, its pod, its node. Evidence can
*block* a transition (no routable execution → no completion) but can never
*authorize* one; only intent plus a fence can. The interface makes that
split a type-system fact instead of a doc bullet: transitions live on
`PlacementStore`, evidence writes live on `PlacementEvidence` (`bind_pod`),
so a component holding only the evidence surface has no operation that can
move placement state. Note what falls out:
`suspended` is just unplaced-with-an-artifact, `paused` and `started` are
the same placement state distinguished only by observed runtime, and a
"wedged `restoring`" cannot be stored at all — `Placing` is derived from a
live lease, so an expired lease *derives* back to unplaced-with-artifact
with nothing to repair.

Honest caveat about today: the operations still guard on the legacy
`lifecycle` strings, because during migration the unported writers and
readers speak that vocabulary and the machine must defend against writers
it hasn't absorbed yet. The dependence is deliberately quarantined — typed
at the boundary, one named blocking set, all inside this crate's SQL — so
when every writer routes through here, swapping the guards onto
placement-native columns is a change to one crate, and `lifecycle` becomes
a rendered projection of the table above.

### The legacy transitions these operations replace

| From | To | Writer (service) | New transition |
| --- | --- | --- | --- |
| `creating` | `created`/`started` | space-api (create handler) | `Unplaced → Placing → Placed` (`create`, `claim_placement`, `complete_placement`) |
| `suspended` | `restoring` | space-api (wake/restore handler) | `Unplaced → Placing` (`claim_placement(UserRestore)`) — implemented; the user wake path cuts over in the next PR |
| `suspended`/`restoring` | `restoring` (re-drive) | cluster-controller (node-drain sweep) | `Evacuating`, restore attempt claimed (`claim_placement(EvacuationRestore)`) — implemented; the drain caller cuts over in the next operation PR |
| `restoring` | `started` | space-api (two CAS copies: post-wake, redrive; the pooled post-wake update refuses `restoring`) | `Placing → Placed` (`complete_placement` — the fenceless redrive adopts under `PlacementProof::LapsedClaim` once the cancelled claim's lease lapses) — implemented; the post-wake and redrive writers cut over in the next PR |
| `restoring` | `started`/`paused` (preserved target) | cluster-controller (drain finalize) | `Evacuating → Placed` (`complete_placement`) — implemented; the drain finalize cuts over in the next operation PR |
| `started`/`paused` | `suspending` | space-api (suspend handler) | still `Placed` — suspend begins, placement changes only when the artifact is durable (`begin_suspend`) |
| `suspending` | `suspended` | space-api + cluster-controller (evacuation suspend commit) | `Placed → Unplaced` + artifact (`complete_suspend`) |
| `restoring` | `suspended`/`failed` (give-up, aging) | cluster-controller (transient sweeper, repair loops) | `Placing → Unplaced` + artifact (`abandon`) — or no writer at all: an expired lease already reads as claimable |
| `started`/`paused` | `stopping` → `stopped` | space-api (stop handler) | `Placed → Unplaced`, no artifact |
| any | `deleting` → `deleted`/`cleanup_required` | space-api (delete) + cluster-controller (orphan sweep) | leaves the machine: delete is an intent, then a tombstone (`finalize_delete`) |
| any | `failed` | space-api, cluster-controller, internal-api (status ingest) | `Placing/Placed → Unplaced` + recorded loss (`abandon`) |
| any | `quarantined` | internal-api (quarantine store) | no placement change — a hold overlay on whatever state the row is in (`quarantine`) |
| `suspended` | marked for evacuation | cluster-controller (node-drain mark) | `Unplaced`+artifact or `Placed` `→ Evacuating` (`evacuate`) |
| residents of a dead node | give-up states | cluster-controller (drained-node terminator) | `Placed/Evacuating → Unplaced` — artifact if one is durable, recorded loss if not (`node_loss`) |
| anything | `started` etc. **written from node publishes** | internal-api (published-state ingest, node-reported) | **no transition** — publishes become evidence, input to guards, never authority |
| `started` | `pausing`/`paused`/`resuming` | space-api (pause/resume handlers) | `Placed → Placed` — not a placement transition; observed runtime is the node's to report |

### Reading the four states from Postgres

In the end state this is not a derivation: **`placement` is a stored enum
in the existing `sandboxes.placement` column —
`unplaced | placing | on_node | evacuating`, with `on_node` kept as
`Placed`'s stored spelling so adoption never rewrites hot rows — written
only by this crate's operations, and reading it is `SELECT placement`.**
The lease, artifact, node, and marker columns are facts *about* the state,
tied to it by the operations' transactions (an `on_node` row always has its
node; a `placing` row always has a live-or-expired lease). The migration
that widens the column's vocabulary (a `CHECK` added `NOT VALID`, then
`VALIDATE`d — the repo's SQL rules for the hot `sandboxes` table) lands
with the phase-4 guard-swap PR, not before: until every writer routes
through this crate, nothing writes the new values.

The predicate table below is two other things, not the read path:
the **migration formula** — how the true state is computed from today's
columns while legacy writers still run (the shadow-compare and the
eventual backfill) — and afterward a **consistency alarm**: stored state
and derived state must always agree, and a disagreement is a bug, not an
input.

| State | Derivation from today's columns (first match, in order) |
| --- | --- |
| `Evacuating` | `evacuation_restore_target IS NOT NULL`, or the resident node's drain phase is active |
| `Placing` | `restore_lease_token IS NOT NULL AND restore_lease_expires_at > NOW()` — a live fence *is* an owned attempt |
| `Placed` | `placement = 'on_node' AND active_node_id IS NOT NULL`, evidenced by a routable active execution |
| `Unplaced` | none of the above; the durable-snapshot predicate decides suspended-versus-stopped rendering |

The precedence encodes the semantics — a drain in flight outranks
everything; a live lease outranks a stale column — and the claim guards
keep the wedge unstorable either way: a stored `placing` whose lease has
expired is claimable by definition, so nothing ever needs a sweeper to
"fix" it.

## What the crate refuses to do

- **No get/put/query surface.** You cannot fetch a row, edit it, and write
  it back; every mutation is an operation with its guards inside the SQL.
  Illegal transitions have no function to call.
- **No new methods for variations.** The trait's verb set is capped by the
  spec's operation inventory; a variation of an operation — a different
  proof of ownership, a claim protocol, a drain step — is a parameter on
  its verb (`PlacementProof`, `ClaimKind`, `EvacuationStep`), never a sibling
  method, and growing the trait requires a spec amendment first. One
  completion verb, `complete_placement`, serves both the fence holder and the
  lapsed-attempt adoption.
- **No trusting the caller.** Holding the fence proves you may complete;
  it does not prove your restore worked. Completion demands evidence.
- **No guessing on bad data.** A row whose lifecycle column holds something
  outside the vocabulary fails *that row's* operation loudly rather than
  being treated as unplaced — a guessed "unplaced" fed to a repair loop is
  how duplicate pods happen.
- **No permanent limbo.** Every refused operation leaves the row in a state
  some legal next actor can advance: leases expire, user demand re-drives,
  sweeps re-select.

## Storage

Reads and writes `sandboxes` and `sandbox_k8s_executions` (plus a read of
the `node_k8s`/`nodes` mapping at completion). The deprecated pod
compatibility projection is deliberately untouched. There is no kube client
dependency here — the Kubernetes-ness is names and columns; the clients
that create and watch pods live in the api substrate
(`crates/l40_api/api/src/controllers/substrate/kubernetes/`).

One scope decision to know: this crate is explicitly the **Kubernetes**
placement machine. Pods, kube node names, and the k8s execution table
appear by name in its types and its SQL; Direct-substrate sandboxes keep
their legacy path. Generalizing the interface waits for a real second
substrate to generalize over — an abstraction derived from one example
would just be a synonym. The one abstraction the crate does keep is
`NodeIncarnation`, because node identity is the one noun already scheduled
to churn: today it is the kube node UID, the v2 schema replaces it with
`node_incarnations` rows, and that swap stays inside this crate.

## Where the details live

- Interface reference: the rustdoc on `PlacementStore` and the outcome
  enums — the doc comments sit beside the SQL they describe and are
  reviewed with it. This README deliberately repeats none of the
  signatures.
- Design and invariants: the crate documentation in `src/lib.rs`.

## Testing

`src/postgres/tests/` runs the implemented operations — the restore-shaped
claims and both completion proofs — against a real migrated Postgres in a
testcontainer; the declared-ahead operations are covered by typed-refusal
tests in `src/postgres/tests.rs`:

```bash
cargo nextest run -p pplx-space-placement
```

The suites skip silently without a Docker daemon — a green run that
skipped them is not a green run.
