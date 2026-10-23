//! The termination fence: the one place a placement writer and the
//! provider-terminate path are ordered against each other.
//!
//! # The hole this closes
//!
//! `POST /admin/api/v1/nodes/{id}:terminate` and
//! `drained_node_terminator::mark_terminating` both commit
//! `drained → terminating` under the `nodes` row lock, having counted occupancy
//! in the same transaction. That is sound as far as it goes, and it goes exactly
//! as far as the transactions that also lock that row.
//!
//! `sandboxes.active_node_id` has no foreign key to `nodes(id)`, so attaching a
//! sandbox to a node takes no lock on the node. Five writers set it without
//! touching `nodes` at all — pooled post-wake, post-create placement, the
//! create/restore finalizer, pooled create, and direct-substrate restore — and a
//! sixth (`persist_pod_node_placement`) held the lock through its `nodes` upsert
//! but never read the phase back. Nothing on the `terminating` path re-consults
//! the resident guard, because that guard rides the `drained → terminating` edge.
//! So a placement committing after the phase write reached the provider call and
//! the sandbox was destroyed with the instance.
//!
//! `node_drain::handle_terminating_node` recounts the terminator's blocking set
//! immediately before the provider call and reverts to `drained` instead of
//! terminating. That shrank the exposure from a whole reconciler tick to the
//! microseconds between the recount and the provider call. It did not remove it:
//! a standalone final count cannot order anything.
//!
//! # The shape
//!
//! Two obligations, one on each side, and the `nodes` row lock is the only thing
//! between them:
//!
//! * **The terminate side** takes `FOR UPDATE` on the `nodes` row *in its own
//!   statement* before writing the phase, and re-checks residency after that.
//!   `set_drain_phase`'s `draining → drained` edge, `mark_terminating`, and the
//!   admin `:terminate` handler all do.
//! * **The placement side** calls [`fence_node_for_placement`] in the same
//!   transaction that commits `active_node_id`, and abandons the write when it
//!   answers anything but [`PlacementFence::Open`]. Six call sites, enumerated in
//!   `pplx_space_api`'s `controllers::placement_fence::writer` — one per
//!   statement, not one per code path, because two of them are two statements on
//!   the same create.
//!
//! Whichever transaction reaches the row first wins, and the loser is refused:
//!
//! * *Placement first.* It holds `FOR KEY SHARE`, so the phase write's
//!   `FOR UPDATE` waits. When it proceeds, its residency re-check runs as a new
//!   statement and takes a fresh snapshot that includes the committed
//!   placement — so it matches zero rows and the phase is never written. No
//!   provider call follows a phase that was not written.
//! * *Phase write first.* The placement's `FOR KEY SHARE` waits. Under READ
//!   COMMITTED a locking `SELECT` re-reads the row after acquiring the lock, so
//!   it observes the committed `drained`/`terminating` and refuses.
//!
//! # Why `FOR KEY SHARE` and not something stronger
//!
//! The objection to locking the `nodes` row from the placement path is
//! contention on a row the terminator also `DELETE`s from. `FOR KEY SHARE` is
//! the weakest mode that still answers it. It conflicts with `FOR UPDATE` and
//! with `DELETE` — the terminate path and node teardown — and with nothing else
//! on this row:
//!
//! | Concurrent writer of this `nodes` row | Lock it takes | Conflicts? |
//! | --- | --- | --- |
//! | admin `:terminate` / `set_drain_phase` / `mark_terminating` pre-lock | `FOR UPDATE` | yes — the ordering this exists for |
//! | `DELETE FROM nodes` (node teardown, orphan reap) | `FOR UPDATE` | yes |
//! | the internal API's registration/heartbeat upsert, the kube inventory reflector, the `nodes` upsert inside pod placement | `FOR NO KEY UPDATE` (an `ON CONFLICT DO UPDATE` that touches no key column) | no |
//! | another placement writer through this helper | `FOR KEY SHARE` | no |
//!
//! **Measured, not inferred.** Every row of that table was checked against a
//! live Postgres 16 by holding one mode and timing out the other, including the
//! `ON CONFLICT (id) DO UPDATE … WHERE <nothing changed>` shape pod placement
//! uses — which does *not* block `FOR KEY SHARE` but does block `FOR UPDATE`,
//! so the terminate path stays ordered against it while placements do not
//! serialize on each other. The one that would have been easy to get wrong by
//! reasoning is that same upsert: `ON CONFLICT DO UPDATE` locks the conflicting
//! tuple even when its `WHERE` writes no new version, and the strength depends
//! on whether key columns are in the `SET` list.
//!
//! So concurrent placements onto one node do not serialize with each other, and
//! the ordinary write traffic against a node's row is untouched.
//!
//! # Why not enforce this once, in the schema
//!
//! Six call sites is an enumeration, and an enumeration can be forgotten. Three
//! deeper places to put it were considered; each is rejected for a reason
//! specific to this codebase rather than on principle.
//!
//! **A foreign key.** An FK checks *existence*, and this fence's entire subject
//! is *phase* — so an FK buys the lock ordering and none of the verdict, and every
//! writer would still need its own phase check. That alone decides it.
//!
//! On the delete action: a `SET NULL` reference to `nodes(id)` is *not* unheard of
//! in this schema, so "you cannot reference `nodes`" is not the objection — an
//! earlier version of this comment claimed it was. But the count that replaced it
//! has been wrong twice, so it is gone: enumerating live FK actions from migration
//! files is a fact that rots and that nothing here needs. One citation is worth
//! keeping because it cuts *against* SET NULL rather than for it —
//! `sandbox_observed_states.node_id` was deliberately moved from SET NULL to
//! `ON DELETE CASCADE`.
//!
//! What actually rules SET NULL out here needs no count at all: it would produce a
//! row with `placement = 'on_node'` and `active_node_id IS NULL`, precisely the
//! shape `node_drain::store`'s reverse-order comment calls invisible to every term
//! of the drain gate. The FK would trade a visible phantom for an invisible one.
//! `ON DELETE CASCADE` is worse again: the terminator's `DELETE` of a node row
//! would delete the rows of every sandbox still pointing at it.
//!
//! **A `BEFORE UPDATE OF active_node_id ON sandboxes` trigger.** The precedent
//! exists — several rejecting `BEFORE UPDATE` triggers on this table already
//! ship — and it would even reach the one writer this fence cannot (see below),
//! because a `BEFORE` trigger sees `NEW.active_node_id` already resolved. It is
//! rejected because **it inverts the lock order.** A row trigger fires after the
//! `sandboxes` row is locked, so the `nodes` lock would be taken *second* — and
//! node-then-sandbox is the ordering that makes this set of transactions
//! cycle-free (see [`fence_node_for_placement`]). Every writer would take the two
//! rows in both orders depending on which path it came through. Two lesser costs
//! follow: a trigger turns a typed verdict into a SQLSTATE that all six sites
//! must sniff and map back, so the enumeration is relocated rather than removed;
//! and disabling it means `ALTER TABLE ... DISABLE TRIGGER` on a hot table
//! instead of an env var.
//!
//! **One store function every writer funnels through.** Ruled out in writing by
//! the owning crate: `pplx_space_sandbox_store`'s own boundaries say "guarded
//! placement transitions are not CRUD ... they stay named, guarded operations
//! wherever they live" and "locking reads are not gets." Three of the six
//! statements carry a CAS in the same `UPDATE`, so splitting them into "write the
//! row" plus "write `active_node_id`" would break the atomicity that makes them
//! correct.
//!
//! What is left is the enumeration plus a mechanical check on it —
//! `scripts/source_guards/placement_fence_writers.py`, which fails CI when a
//! production statement assigns a non-NULL `active_node_id` in a function that
//! never mentions this module.
//!
//! # Which phases fence, and why `draining` is not one of them
//!
//! `drained` and `terminating`, plus a phase this build cannot parse. **The rule
//! is that refusing must not orphan the sandbox from the evacuation pipeline**,
//! and that rule picks out exactly those:
//!
//! * Node-drain lists only `draining` nodes, and its evacuation work queue selects
//!   `WHERE s.active_node_id = $1`. On a `draining` node the write this fence
//!   would refuse *is* the write that makes the sandbox visible to
//!   suspend-and-restore-elsewhere. Refusing does not stop the pod — it is already
//!   there — it removes the sandbox from the queue that would have saved it, and
//!   the node drains and terminates with a guest nothing is looking for. That is
//!   strictly worse than recording the placement.
//! * On `drained` and `terminating` no evacuation runs, so refusing removes the
//!   row from nothing, and the sandbox is lost either way. Recording it there only
//!   adds a row claiming a healthy sandbox on an instance about to stop existing.
//!
//! **This is not [`pplx_space_common_core::types::NodeDrainState::blocks_placement`],
//! and an intermediate revision that delegated to it was wrong.** That function is
//! right for *admission*, which refuses a user call before a pod exists, where
//! refusing is free. This fence runs after the pod may already be on the node, so
//! the identical verdict has the opposite effect. The two diverge on one cell —
//! `draining` + forced — and they diverge because they are asked at different
//! points in a sandbox's life. The claim that this fence "makes the admission
//! answer authoritative at commit time" was wrong in both directions: as a
//! description before the delegation, and as a goal after it.
//!
//! # What this fence broke elsewhere, and what it did not
//!
//! Adding `FOR KEY SHARE` to the placement path created a lock conflict that did
//! not exist before, so statements that could never block on a placement writer
//! now can — and any such statement whose guard is a `NOT EXISTS`/`EXISTS` over a
//! **different table** goes stale when it blocks, by the same READ COMMITTED rule
//! [`lock_node_for_phase_write`] exists for. Every `DELETE FROM nodes` in the tree
//! was audited against that:
//!
//! * `orphaned_nodes::store`'s reap — **fixed here.** Its guard is a `NOT EXISTS`
//!   over `sandboxes` and its candidate has `drain_phase IS NULL`, so the fence
//!   returns `Open`, the placement *commits*, and the guest is live. It got the
//!   same `FOR UPDATE` preamble.
//! * `drained_node_terminator::store`'s orphan reap — **fixed there**, same shape,
//!   milder because that path runs only after EC2 confirmed the instance gone.
//! * `tombstone_retention` — same shape, **deliberately not changed**, and the
//!   reason is stronger than the shape. Its candidate CTE (in
//!   `tombstone_retention.rs`) is not "nodes already tombstoned": it is
//!   `managed_by = 'space'` direct nodes whose `node_direct.deleted_at` is past a
//!   retention window, whose `nodes.state` is already `deleted`, **and** which
//!   carry no `sandboxes` row referencing them through either `active_node_id` or
//!   `target_node_id` outside `('deleted','failed')`. So the residency check this
//!   fence exists to order against is already a term of the candidate set. That
//!   `NOT EXISTS` is evaluated on the delete's own snapshot, not ordered against
//!   a concurrent placement, so this rests on nothing placing onto a direct node
//!   whose `node_direct.deleted_at` is past retention. Its multi-row
//!   `DELETE … USING candidates` also has no single row to pre-lock. Noted rather
//!   than fixed so the omission is a decision rather than a gap.
//! * `admin::node_maintenance`'s direct-node delete — **unaffected.** Its `EXISTS`
//!   is over `node_direct`, which no placement writer touches.
//! * `node_drain::store`'s `delete_terminating_node` — **unaffected**, and it is
//!   the instructive case: its guard is entirely on the `nodes` row it deletes,
//!   which is exactly what EvalPlanQual *does* re-check after a lock wait. The
//!   staleness only bites guards that reach across tables.
//!
//! # A missing `nodes` row refuses
//!
//! Provider termination APIs acknowledge the request before the underlying
//! compute is necessarily gone, and the drain loop deletes the `nodes` row as
//! soon as that acknowledgement returns. A create or restore that began before
//! termination can therefore reach its placement commit after the row delete
//! while the departing machine still exists. Treating absence as open would
//! recreate the destructive window on the far side of the terminal row state.
//!
//! This is deliberately narrower than a foreign key. Existing phantom
//! `active_node_id` rows remain modelled for recovery and census discounting;
//! the fence only prevents these enumerated live writers from creating a new
//! phantom after the target has lost its control-plane identity.
//!
//! One writer can still see a row where there should be none. `pod_placement`
//! upserts the `nodes` row from Kubernetes before it fences, after a tombstone
//! check in a separate statement; a terminate-side delete committing between the
//! two lets the upsert recreate the row with no `drain_phase`, which the fence
//! then reads as open. That ordering predates the fence and is not closed here.
//! The kill switch
//! still restores the exact prior no-lock/no-check behavior.
//!
//! # What it does not close
//!
//! The pod keeps running. Refusing the placement write does not stop a container
//! that Kubernetes already started on the node; it makes the *operation* fail,
//! and a failed operation is what gets the sandbox off a dying node — instead of
//! a row claiming a healthy sandbox on an instance that is about to stop
//! existing. What "fail" means is per call site and not uniform: the two create
//! paths queue a durable compensation that tears the sandbox down and return a
//! retryable 503, the pooled create releases its warm slot, and direct restore
//! and pod placement report `RestoreInFlight`. On a Kubernetes cold create, pod
//! placement's refusal is a teardown too, run by the create's cancel guard: it
//! claims the row as `deleting`, deletes the pod, and then the row. When that
//! claim isn't confirmed the row is kept, because Kubernetes may already have
//! started the pod and the row is how its cleanup finds it. The invariant across
//! all of them is only that no refusal returns success.
//!
//! And the enumeration is a list, not a schema constraint — which is not a
//! hypothetical: writing this fence turned up two `active_node_id` writers that
//! an initial list of four had missed. One is fenced
//! (`finalize_created_sandbox`, the only *controller-level* such write left on
//! the restore path, where the placement-only `UPDATE` is skipped; the
//! Kubernetes substrate also writes the column earlier from the pod-IP watch,
//! and that writer is fenced too). **The other is not, and
//! is named here rather than left to be rediscovered:**
//!
//! > `pplx_space_placement`'s `complete_placement` (`postgres.rs`'s
//! > `completion_sql`) sets `active_node_id = placed.node_id`. It has **no
//! > production caller** — that crate's own docs say its operations "land one per
//! > PR" and the api's wake path has not cut over — so nothing reaches it today.
//! > It is not fenced here because it resolves the node *inside* the UPDATE, from
//! > a `sandbox_k8s_executions` join, so a correct fence needs the node resolved
//! > and locked in a prior statement, which is a restructure of that statement's
//! > atomicity rather than a call added to it. Whoever lands its cutover owns
//! > that; a phase predicate without the prior lock would be the half-measure this
//! > whole fence exists to reject. The source guard's allowlist is where it is
//! > recorded, so it cannot be forgotten silently.
//!
//! For the general case the enumeration is checked mechanically rather than by
//! attention: `scripts/source_guards/placement_fence_writers.py` fails CI when a
//! production statement assigns a non-NULL `active_node_id` in a function that
//! never mentions this module, and it pins the site count so a scanner that goes
//! blind fails instead of passing. Its allowlist is where a knowingly-unfenced
//! writer is recorded, so the paragraph above and the guard cannot drift. What it
//! cannot check is that the fence is consulted *before* the write or that its
//! verdict is honoured — that is what the interleaving tests and the per-writer
//! tests are for, and `handle_terminating_node`'s pre-provider recount still
//! stands behind all of it as defence in depth.

use pplx_space_common_core::{env::env_flag_var, sql, types::NodeDrainPhase};
use std::sync::OnceLock;
use std::time::Duration;
use uuid::Uuid;

/// Kill switch. Read through the shared flag vocabulary; only a recognized falsy
/// value disables the fence — see [`placement_fence_enabled`].
pub const PLACEMENT_TERMINATION_FENCE_ENV: &str = "SPACE_PLACEMENT_TERMINATION_FENCE";

/// Whether [`fence_node_for_placement`] enforces, read once per process from
/// [`PLACEMENT_TERMINATION_FENCE_ENV`] through the shared flag vocabulary in
/// `pplx_space_common_core::env`.
///
/// **Defaults to on, and the default is the point.** The fence only refuses after
/// a node has left the evacuation-capable `draining` phase for `drained`,
/// `terminating`, or an unknown phase. There is no safe partial population to
/// soak: shipping it off would leave a live data-loss path open behind a flag
/// nobody is scheduled to flip. What the switch buys is a way to restore the
/// exact prior behaviour (no lock, no check) without a rollback, if the added row
/// lock turns out to interact with something this analysis missed.
///
/// [`env_flag_var`] rather than a local `matches!`, and `true` rather than
/// [`pplx_space_common_core::env::env_destructive_gate_var`], for reasons that
/// are both about what a typo does:
///
/// * The shared vocabulary is shared so that one string means one thing
///   everywhere. A local list drifts — an earlier version of this function
///   omitted `disabled`, which the shared falsy set accepts, so
///   `SPACE_PLACEMENT_TERMINATION_FENCE=disabled` would have left the fence
///   *enforcing* while the same token disabled every other switch in the fleet.
///   That is the worst possible direction for a knob someone reaches for during
///   an incident.
/// * `env_destructive_gate` reads an unrecognized value as *off*, which is right
///   for gating a destructive path and wrong here: this is the guard against the
///   destruction, so a typo must leave it on. `env_flag(raw, true)` gives exactly
///   that — only a recognized falsy value disables.
///
/// **Do not set the env var from a test and expect this to see it.** The first
/// caller in a test binary fixes the value for the whole process. Tests that
/// need a specific policy pass `enabled` to [`fence_node_for_placement`] directly.
pub fn placement_fence_enabled() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| env_flag_var(PLACEMENT_TERMINATION_FENCE_ENV, true))
}

/// What [`fence_node_for_placement`] found once it held the row lock.
///
/// Flat rather than `Fenced { reason }`: nothing outside [`Self::as_str`] ever
/// destructured the reason, and the nesting read badly where it mattered most —
/// a caller's own refusal variant wrapping this one produced
/// `Fenced(PlacementFence::Fenced { reason: .. })`, with both layers spelled the
/// same.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlacementFence {
    /// The node accepts the placement: `drain_phase` is NULL or `draining`, the
    /// sandbox is already on this node, or the fence is disabled by
    /// [`placement_fence_enabled`].
    Open,
    /// The target has no `nodes` row. Provider termination may only have been
    /// accepted, not completed, so late placement must fail closed rather than
    /// publish a new phantom reference onto departing compute.
    MissingNode,
    /// `drain_phase = 'drained'`: the node is one terminator tick from the
    /// provider call. An attach here is a park rather than a destroy, and is
    /// refused for that reason — the terminator would refuse the node on every
    /// tick from then on.
    Drained,
    /// `drain_phase = 'terminating'`: the provider call is imminent and may
    /// already have gone out. This is the destructive arm.
    Terminating,
    /// A non-NULL `drain_phase` this build cannot parse, refused fail-closed. Its
    /// own variant rather than being folded into [`Self::Terminating`], so a log
    /// line and a metric tag never carry a phase the database did not hold.
    UnrecognizedPhase,
}

impl PlacementFence {
    /// True when the caller must abandon the `active_node_id` write.
    pub const fn refuses(self) -> bool {
        !matches!(self, Self::Open)
    }

    /// Stable tag for metrics and log fields.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::MissingNode => "missing_node",
            Self::Drained => NodeDrainPhase::Drained.as_str(),
            Self::Terminating => NodeDrainPhase::Terminating.as_str(),
            Self::UnrecognizedPhase => "unrecognized_phase",
        }
    }
}

/// The terminate half of the fence: take `FOR UPDATE` on the `nodes` row, in its
/// own statement, with a bounded wait.
///
/// Both writes that *enter* a fenced phase need this and neither can skip it,
/// for two reasons that are easy to state and easy to get wrong:
///
/// * Their residency guards are `NOT EXISTS` over `sandboxes` — *other* rows.
///   Under READ COMMITTED an UPDATE that blocks on this row's lock re-checks the
///   row it is updating but, in Postgres's words, "does not see effects of those
///   commands on other rows in the database." Taking the lock in a **prior
///   statement** is what makes the guarded UPDATE start with a snapshot that
///   includes whatever committed while it waited.
/// * A bare `UPDATE nodes` takes `FOR NO KEY UPDATE`, which does **not** conflict
///   with the `FOR KEY SHARE` the placement writers take. The explicit
///   `FOR UPDATE` is the thing they are ordered against.
///
/// The wait is bounded by `timeout` and the caller classifies the resulting
/// `55P03` as retryable — the phase writers iterate candidates sequentially, so
/// an unbounded wait on one contended row stalls every node behind it, and one
/// node losing a tick is the right price. The caller's prior `lock_timeout` is
/// restored after the row lock is acquired so later statements cannot be
/// mistaken for a timeout from this preamble.
///
/// `timeout` is a parameter rather than a shared constant on purpose: the phase
/// writers have different tick budgets, and a future change to one writer's
/// patience must not silently move the others. It is the bounded
/// lock-before-write protocol that is shared here, not the value — those are
/// different things, and an earlier version of this code conflated them and
/// shipped the protocol twice.
pub async fn lock_node_for_phase_write(
    conn: &mut sqlx::PgConnection,
    node_id: Uuid,
    timeout: Duration,
) -> Result<(), sqlx::Error> {
    let previous_timeout: String =
        sqlx::query_scalar(sql!("SELECT current_setting('lock_timeout')"))
            .fetch_one(&mut *conn)
            .await?;
    // Transaction-local, and bound from a typed `Duration` rather than a SQL
    // literal so the value cannot drift from the constant that documents it.
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(timeout.as_millis().to_string())
        .execute(&mut *conn)
        .await?;
    sqlx::query(sql!("SELECT 1 FROM nodes WHERE id = $1 FOR UPDATE"))
        .bind(node_id)
        .execute(&mut *conn)
        .await?;
    sqlx::query(sql!("SELECT set_config('lock_timeout', $1, TRUE)"))
        .bind(previous_timeout)
        .execute(conn)
        .await?;
    Ok(())
}

/// True iff `error` is Postgres `55P03` (`lock_not_available`) — a
/// [`lock_node_for_phase_write`] timeout rather than a database problem.
///
/// Load-bearing for log volume, not tidiness: without it a node whose row is held
/// by a long transaction produces one `set_drain_phase failed` WARN per
/// reconciler tick — hundreds an hour, in the same channel the per-node warn
/// pacer exists to protect — and it reads identical to a real outage for anyone
/// alerting on it.
pub fn is_lock_timeout(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|error| error.code())
        .is_some_and(|code| code == "55P03")
}

/// What a writer offers as proof that its write *re-states* an existing
/// placement rather than creating one.
///
/// The carve-out exists because refusing some writers makes the caller tear down
/// a live sandbox, so a re-statement has to be let through. That trade only holds
/// if the write really is a re-statement, and `active_node_id` alone is too weak
/// to establish it: a row can name a node because a *previous* execution ran
/// there, while the write in hand belongs to a fresh one. `sandboxes` keeps no
/// execution identity, so the sandbox id cannot distinguish the two cases and a
/// writer that can has to say so.
#[derive(Debug, Clone, Copy)]
pub struct Reaffirmation<'a> {
    /// The sandbox whose placement is about to be written.
    pub sandbox_id: Uuid,
    /// The Kubernetes node name this write is recording, when the caller is the
    /// pod-placement writer.
    ///
    /// `Some` tightens the carve-out: the row must name the node **and** the
    /// sandbox's *active* `sandbox_k8s_executions` row must already name it too.
    /// That is what makes it a re-statement of this execution's placement rather
    /// than an inherited one.
    ///
    /// `None` is for writers with no execution to name — the create/restore
    /// finalizer and the pooled post-wake path — which fall back to the node
    /// match. The finalizer runs on *both* substrates, not only the direct one;
    /// what it lacks is a pod, not a substrate. See [`fence_node_for_placement`]
    /// for why the node match alone is sound for them.
    pub active_k8s_node_name: Option<&'a str>,
}

/// Lock the `nodes` row against the provider-terminate path and report whether
/// it still accepts a placement.
///
/// Call this in the **same transaction** that commits `active_node_id`, before
/// that write. A separate transaction proves nothing: the lock is what orders
/// the two, and it is released at commit.
///
/// Take it **before** any `sandboxes` row lock the same transaction needs.
/// Every fenced writer locks `nodes` then `sandboxes`, which is the order
/// `persist_pod_node_placement` already used, so there is no cycle for two of
/// them to deadlock on.
///
/// A disabled fence returns [`PlacementFence::Open`] without issuing a
/// statement, so the kill switch removes the lock as well as the check.
///
/// `reaffirmation` names the sandbox whose placement is about to be committed,
/// when the caller wants the re-affirmation carve-out — see `pplx_space_api`'s
/// `check_placement_fence`, which is what production calls. Passing `None` asks
/// the pure question: is this node placeable at all.
///
/// `enabled` is a parameter rather than read inside, because
/// [`placement_fence_enabled`] caches per process and the first caller in a test
/// binary would fix it for every later test. Production passes
/// `placement_fence_enabled()`; tests pass the policy they mean.
pub async fn fence_node_for_placement(
    conn: &mut sqlx::PgConnection,
    node_id: Uuid,
    reaffirmation: Option<Reaffirmation<'_>>,
    enabled: bool,
) -> Result<PlacementFence, sqlx::Error> {
    if !enabled {
        return Ok(PlacementFence::Open);
    }
    // **Two statements, and the split is load-bearing — not a missed
    // optimization.** An earlier revision merged these into one locking `SELECT`
    // with a correlated `EXISTS` over `sandboxes`, on the reasoning that one
    // statement makes the two facts share a snapshot. It does the opposite when
    // the statement blocks, which is exactly when this fence matters.
    //
    // `FOR KEY SHARE OF n` triggers EvalPlanQual on the `nodes` tuple, so the
    // phase is re-read from the newest committed version. But EPQ runs the rest
    // of the plan against the *parent* statement's snapshot — the one taken
    // before the wait — so a correlated subquery over another table stays stale.
    // Measured on Postgres 16: with a phase writer holding the row and a detach
    // committing mid-wait, the merged statement returned `drain_phase = 'drained'`
    // (fresh) and `already_placed = true` (stale, three seconds out of date). The
    // carve-out then fired on that stale `true` and passed a *first* attach onto a
    // `drained` node without ever obtaining a verdict.
    //
    // This is the same READ COMMITTED rule the phase writers' guards obey by
    // taking their lock in a prior statement (see [`lock_node_for_phase_write`]).
    // A second statement gets a fresh snapshot; that is the whole mechanism.
    // **No explicit `lock_timeout` here, unlike the terminate half — and the
    // asymmetry is deliberate, not an omission.** This statement's wait *is*
    // bounded: `space_user` carries a role-level `statement_timeout` of 10s
    // (`bootstrap_roles.rs`'s `app_statement_timeout` default, applied to
    // `USER_ROLE` in `converge_session_timeouts`), and `statement_timeout` covers
    // lock acquisition, not just execution. Note it is `statement_timeout` that
    // does this: `space_user` has no role-level `lock_timeout` — those are two
    // different knobs, and reading the missing one as "no bound" is the easy
    // mistake here.
    //
    // [`lock_node_for_phase_write`] then bounds itself *tighter* than the role
    // for a reason this path does not share: its callers are reconcilers that
    // iterate candidate nodes sequentially, so one contended row stalls every
    // node queued behind it, and they need `55P03` distinguishable so the node
    // can be deferred and retried rather than counted as failed. A request path
    // has neither property — it holds up only itself, and its caller already
    // retries onto another node.
    //
    // What the asymmetry costs today is **classification, not availability.** A
    // fence wait that loses to a long `DELETE FROM nodes` cascade expires as
    // `57014` (`statement_timeout`) on a user request — indistinguishable from
    // any other slow query — where the same contention on the terminate side
    // arrives as `55P03`, is recognized by [`is_lock_timeout`], and is counted as
    // a deferral rather than a failure. So this path is bounded but not
    // *legible*: contention here shows up in the API's generic timeout rate, not
    // as a fence signal.
    //
    // **"Today" is load-bearing, and this is the paragraph to read before
    // enabling `SPACE_DB_APP_LOCK_TIMEOUT`.** The `57014` claim above holds only
    // while `space_user` has no role-level `lock_timeout`, which is explicitly
    // not the intended end state:
    // `bootstrap_roles`'s own doc calls enabling it a per-environment rollout to
    // a sub-second value. Once it is sub-second anywhere, this `SELECT` can raise
    // `55P03` instead. The two substrate writers, `direct_restore` and
    // `pod_placement`, classify it through the API's `DbContext` as the
    // retryable `LockContention`. The four controller writers don't classify it
    // at the fence. For `post_create_placement` and `create_finalize`, the `?`
    // here would carry momentary contention into the create path's generic
    // error arm, which runs `rollback_created_sandbox`, so a sandbox whose
    // substrate create already succeeded gets torn down on a node that was
    // never draining. `pooled_create` and `pooled_post_wake` map it to an
    // internal error.
    //
    // So the two knobs are coupled: flipping the app-role `lock_timeout` requires
    // classifying `55P03` at the controller writers first, mapping it to each
    // writer's retryable error rather than to a teardown. Not done here because
    // the mapping differs per writer and this fence has no exposure while the
    // role has no `lock_timeout`.
    let row: Option<Option<String>> = sqlx::query_scalar(sql!(
        "SELECT drain_phase FROM nodes WHERE id = $1 FOR KEY SHARE"
    ))
    .bind(node_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(phase) = row else {
        return Ok(PlacementFence::MissingNode);
    };
    let Some(phase) = phase else {
        return Ok(PlacementFence::Open);
    };
    let verdict = classify(&phase);
    if !verdict.refuses() {
        return Ok(verdict);
    }
    // Only now, and only when we are about to refuse, ask whether this write
    // merely re-states a placement that already exists. Ordering it this way is
    // what makes the split cheaper than the merge it replaces rather than dearer:
    // the probe never runs on the hot path, so a placement onto an ordinary node
    // costs one statement, where both earlier shapes cost two.
    if let Some(Reaffirmation {
        sandbox_id,
        active_k8s_node_name,
    }) = reaffirmation
    {
        // `FOR NO KEY UPDATE` on the sandbox row, so the answer cannot go stale
        // between here and the caller's write. Without it the carve-out has a
        // residual of its own: a detach committing after this read turns a
        // re-affirmation back into a first attach, onto a node this fence has
        // *already decided* to refuse. That is the same shape as the defect this
        // split fixes, one window smaller, and this feature's whole subject is
        // that windows of that shape are reachable.
        //
        // It costs nothing on the hot path: this statement only runs when the
        // verdict already refuses, which for an ordinary node never happens. And
        // it is the lock the caller's `UPDATE` takes anyway, acquired a moment
        // earlier — `nodes` then `sandboxes`, the order every fenced writer uses,
        // so it introduces no cycle.
        let already_placed: Option<bool> = sqlx::query_scalar(sql!(
            "SELECT active_node_id IS NOT DISTINCT FROM $2
               FROM sandboxes WHERE id = $1
                FOR NO KEY UPDATE"
        ))
        .bind(sandbox_id)
        .bind(node_id)
        // Reborrowed rather than moved: the execution probe below may still need
        // the connection, and `&mut PgConnection` is not `Copy`.
        .fetch_optional(&mut *conn)
        .await?;
        if already_placed == Some(true) {
            // Node match established. For pod placement that is not yet enough:
            // the row may name this node because a *previous* execution ran here,
            // and the write in hand may belong to a replacement. Require the
            // active execution to already name the node as well.
            //
            // **Its own statement, for the same reason the phase read is.** Folded
            // into the `SELECT` above it would sit in a plan whose
            // `FOR NO KEY UPDATE` triggers EvalPlanQual on the `sandboxes` tuple,
            // and EPQ re-runs only that tuple — a subquery over
            // `sandbox_k8s_executions` would be answered from the pre-wait
            // snapshot. Same trap as the merged phase probe this module already
            // carries a comment about; the fix is the same shape.
            //
            // No lock needed on the execution row: the statement above holds the
            // `sandboxes` row, and execution mutations take that same row lock
            // first (`lock_sandbox_for_execution_mutation_in`), so this read is
            // stable for the rest of the transaction.
            if let Some(node_name) = active_k8s_node_name {
                let same_execution: Option<bool> = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1
                           FROM sandbox_k8s_executions
                          WHERE sandbox_id = $1
                            AND active
                            AND k8s_node_name IS NOT DISTINCT FROM $2::TEXT
                     )"
                ))
                .bind(sandbox_id)
                .bind(node_name)
                .fetch_optional(conn)
                .await?;
                if same_execution != Some(true) {
                    return Ok(verdict);
                }
            }
            return Ok(PlacementFence::Open);
        }
    }
    Ok(verdict)
}

/// Resolve a committed `drain_phase` into a verdict.
///
/// **The rule: refuse exactly the phases where refusing cannot orphan the sandbox
/// from the evacuation pipeline.** That is `drained` and `terminating`, plus a
/// phase this build cannot parse. `draining` is open *in either mode*, and the
/// asymmetry is the whole point rather than a convenience.
///
/// Node-drain lists only `draining` nodes, and its evacuation work queue selects
/// `WHERE s.active_node_id = $1` (`node_drain::store`'s
/// `evacuatable_sandbox_rows_query`). So on a `draining` node the placement write
/// this fence would refuse is the *same write* that makes the sandbox visible to
/// suspend-and-restore-elsewhere. Refusing it does not stop the pod — it is
/// already on the node — it only deletes the sandbox from the queue that would
/// have moved it to safety, and the node then drains and terminates with a guest
/// nothing was looking for. Pre-fence, that placement was recorded and a forced
/// drain migrated the sandbox. On `drained`/`terminating` no evacuation runs at
/// all, so refusing removes the row from nothing and the "the pod is lost either
/// way" reasoning holds.
///
/// **This is deliberately *not* [`NodeDrainState::blocks_placement`], and an
/// intermediate revision that delegated to it was wrong.** That function is the
/// right answer for *admission* — it refuses a user call before a pod exists,
/// where refusing costs nothing and the sandbox stays where it is. This fence
/// runs after the pod may already be on the node, so the same verdict has the
/// opposite effect. The two diverge on exactly one cell, `draining` + forced, and
/// they diverge because they are asked at different points in a sandbox's life.
/// Saying the fence "makes the admission answer authoritative at commit time" was
/// therefore wrong twice over: as a description of the code before the
/// delegation, and as a design goal after it.
///
/// Two things this keeps of its own, both failing toward refusal:
///
/// * **An unparseable phase refuses.** The current migration has a CHECK over the
///   three known values, but a future phase must widen that constraint before
///   every older binary has stopped running. Fail-closed parsing protects that
///   rolling handoff (and any damaged constraint): over-refusing fails a create
///   that retries onto another node, while under-refusing destroys a sandbox.
/// * **The mode is not read at all.** It was, when this delegated; now that
///   `draining` is open in either mode there is nothing for it to decide, and the
///   column stays out of the statement.
fn classify(raw_phase: &str) -> PlacementFence {
    match NodeDrainPhase::parse(raw_phase) {
        // Open, and load-bearing: this is the phase whose evacuation queue reads
        // the very column the caller is about to write.
        Ok(NodeDrainPhase::Draining) => PlacementFence::Open,
        Ok(NodeDrainPhase::Drained) => PlacementFence::Drained,
        Ok(NodeDrainPhase::Terminating) => PlacementFence::Terminating,
        Err(_) => PlacementFence::UnrecognizedPhase,
    }
}

#[cfg(test)]
mod tests;
