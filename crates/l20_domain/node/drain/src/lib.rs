//! A node drain's vocabulary: its phase, its mode, the engine it runs
//! under, the state the drain gates read, and the admin `:drain` request.

use std::{fmt, str::FromStr};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub const NODE_DRAIN_MODE_FORCED: &str = "forced";
pub const NODE_DRAIN_MODE_GENTLE: &str = "gentle";
pub const NODE_DRAIN_PHASE_DRAINING: &str = "draining";
pub const NODE_DRAIN_PHASE_DRAINED: &str = "drained";
pub const NODE_DRAIN_PHASE_TERMINATING: &str = "terminating";

/// Stage of a node's drain, as stored in `nodes.drain_phase`. NULL in that
/// column means no drain is in progress and has no variant here — model it as
/// `Option<NodeDrainPhase>`.
///
/// The lifecycle is `NULL -> draining -> drained -> terminating -> row
/// deleted`. There is no `deleted` variant: a successful terminate DELETEs the
/// `nodes` row.
///
/// Several modules predate this enum and still carry private `&'static str`
/// phase constants (the cluster controller's `node_drain` reconciler, the
/// admin `:drain` route). New code reads and writes the phase through this
/// type; migrating those constants onto it is a mechanical follow-up kept out
/// of the change that introduced drain modes, so a semantic change to draining
/// and a rename sweep across the drain state machine stay separately
/// reviewable.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeDrainPhase {
    Draining,
    Drained,
    Terminating,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node drain phase '{0}'; expected 'draining', 'drained' or 'terminating'")]
pub struct NodeDrainPhaseParseError(String);

impl NodeDrainPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draining => NODE_DRAIN_PHASE_DRAINING,
            Self::Drained => NODE_DRAIN_PHASE_DRAINED,
            Self::Terminating => NODE_DRAIN_PHASE_TERMINATING,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeDrainPhaseParseError> {
        match value {
            NODE_DRAIN_PHASE_DRAINING => Ok(Self::Draining),
            NODE_DRAIN_PHASE_DRAINED => Ok(Self::Drained),
            NODE_DRAIN_PHASE_TERMINATING => Ok(Self::Terminating),
            other => Err(NodeDrainPhaseParseError(other.to_string())),
        }
    }
}

impl fmt::Display for NodeDrainPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeDrainPhase {
    type Err = NodeDrainPhaseParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<NodeDrainPhase> for String {
    fn from(value: NodeDrainPhase) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeDrainPhase {
    type Error = NodeDrainPhaseParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

#[cfg(feature = "sqlx")]
impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for NodeDrainPhase {
    fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
    }

    fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for NodeDrainPhase {
    fn encode_by_ref(
        &self,
        buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer,
    ) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError>
    {
        <&str as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode(
            self.as_str(),
            buf,
        )
    }
}

#[cfg(feature = "sqlx")]
impl<'r> pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres> for NodeDrainPhase {
    fn decode(
        value: pplx_space_sql_codec::postgres::PgValueRef<'r>,
    ) -> Result<Self, pplx_space_sql_codec::error::BoxDynError> {
        let raw =
            <&str as pplx_space_sql_codec::Decode<'r, pplx_space_sql_codec::Postgres>>::decode(
                value,
            )?;
        Self::parse(raw).map_err(Into::into)
    }
}

/// The engine a node drain runs under, as `:drain` names it and
/// `nodes.drain_engine` records it (NULL is [`Self::V1`]).
///
/// `V1` evacuates each resident from the control plane. `V2` seals the node
/// and lets it make its residents durable itself (sandbox-manager's
/// `DrainNode`), and records what it reports; it runs on Kubernetes only.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum NodeDrainEngine {
    #[default]
    V1,
    V2,
}

/// `nodes.drain_engine` of a v2 drain.
pub const NODE_DRAIN_ENGINE_V2: &str = "v2";

impl NodeDrainEngine {
    /// The `nodes.drain_engine` value: NULL for v1.
    pub const fn column(self) -> Option<&'static str> {
        match self {
            Self::V1 => None,
            Self::V2 => Some(NODE_DRAIN_ENGINE_V2),
        }
    }

    /// Read `nodes.drain_engine`: `v2` is v2, anything else v1. Only a
    /// writer's own bookkeeping reads it this way; a decision about the
    /// node reads [`NodeDrain`], which fails closed on a value this build
    /// doesn't know.
    pub fn from_column(value: Option<&str>) -> Self {
        if value == Some(NODE_DRAIN_ENGINE_V2) {
            Self::V2
        } else {
            Self::V1
        }
    }
}

/// Where a v2 drain stands, from its node's shared columns: the three drain
/// phases, and [`Self::Cancelling`], an undrained v2 drain whose cancel the
/// cluster controller hasn't finished on the node (`drain_engine = 'v2'`
/// with no `drain_phase`). The node may still be sealed until it has.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum V2DrainPhase {
    Draining,
    Drained,
    Terminating,
    Cancelling,
}

/// A node's drain as every gate and reader sees it, built once from its
/// shared columns (`drain_engine`, `drain_phase`, `drain_mode`) by
/// [`Self::from_columns`].
///
/// Carried as one value because every question worth asking depends on all
/// three: a `draining` v1 node blocks user calls only in forced mode; a v2
/// node is sealed in every phase, its cancel included, and refuses every VM
/// activation; and `drained` and `terminating` block under either engine.
/// [`Self::Unknown`] is a combination this build can't read (an engine or a
/// phase it doesn't know), and fails closed: it refuses as much as any drain
/// does.
///
/// Lives here rather than in any one consumer because several must agree:
/// the api-server's user-API drain gates, the internal API's port gate, and
/// the admin `:force-upload-local-snapshots` endpoint, whose byte-stability
/// story is precisely "the drain gate is refusing wakes right now".
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeDrain {
    V1 {
        phase: NodeDrainPhase,
        mode: NodeDrainMode,
    },
    V2 {
        phase: V2DrainPhase,
    },
    Unknown,
}

impl NodeDrain {
    /// Read a node's `(drain_engine, drain_phase, drain_mode)` columns.
    /// `None` when the node has no drain: no engine and no phase. A v2
    /// engine without a phase is a cancel the controller hasn't finished.
    /// An engine or a phase this build doesn't know is [`Self::Unknown`]. A
    /// v2 drain takes no mode, so its `drain_mode` is not read.
    pub fn from_columns(
        engine: Option<&str>,
        phase: Option<&str>,
        mode: Option<&str>,
    ) -> Option<Self> {
        let phase = match phase.map(NodeDrainPhase::parse).transpose() {
            Ok(phase) => phase,
            Err(_) => return Some(Self::Unknown),
        };
        match (engine, phase) {
            (None, None) => None,
            (None, Some(phase)) => Some(Self::V1 {
                phase,
                mode: NodeDrainMode::from_column_str(mode),
            }),
            (Some(NODE_DRAIN_ENGINE_V2), phase) => Some(Self::V2 {
                phase: match phase {
                    None => V2DrainPhase::Cancelling,
                    Some(NodeDrainPhase::Draining) => V2DrainPhase::Draining,
                    Some(NodeDrainPhase::Drained) => V2DrainPhase::Drained,
                    Some(NodeDrainPhase::Terminating) => V2DrainPhase::Terminating,
                },
            }),
            (Some(_), _) => Some(Self::Unknown),
        }
    }

    /// A v1 drain's state from its `drain_phase` and `drain_mode`, for a
    /// reader that doesn't read `drain_engine`.
    pub fn from_v1_columns(phase: Option<NodeDrainPhase>, mode: Option<&str>) -> Option<Self> {
        Self::from_columns(None, phase.map(NodeDrainPhase::as_str), mode)
    }

    /// The drain's phase as `nodes.drain_phase` stores it; `None` for a v2
    /// drain's cancel and for [`Self::Unknown`].
    pub const fn phase(self) -> Option<NodeDrainPhase> {
        match self {
            Self::V1 { phase, .. } => Some(phase),
            Self::V2 { phase } => match phase {
                V2DrainPhase::Draining => Some(NodeDrainPhase::Draining),
                V2DrainPhase::Drained => Some(NodeDrainPhase::Drained),
                V2DrainPhase::Terminating => Some(NodeDrainPhase::Terminating),
                V2DrainPhase::Cancelling => None,
            },
            Self::Unknown => None,
        }
    }

    /// A v1 drain's mode; `None` under any other engine.
    pub const fn mode(self) -> Option<NodeDrainMode> {
        match self {
            Self::V1 { mode, .. } => Some(mode),
            Self::V2 { .. } | Self::Unknown => None,
        }
    }

    /// Whether the drain runs under v2.
    pub const fn is_v2(self) -> bool {
        matches!(self, Self::V2 { .. })
    }

    /// Should every placement-affecting or access call against a sandbox
    /// resolved to this node be rejected, running or not?
    ///
    /// A node in `drained` or `terminating` blocks under either engine and
    /// mode: it is about to be destroyed, so placing or re-placing a sandbox
    /// on it is never right. [`Self::Unknown`] blocks too.
    ///
    /// `draining` is where the engine and the mode decide:
    ///
    /// - **Forced** v1 blocks. The reconciler is actively suspending and
    ///   migrating residents, and an unguarded user call would race that
    ///   pipeline — a wake could re-pin a pod to the cordoned source node
    ///   between the suspend and the restore, or resume a VM the reconciler
    ///   just paused to snapshot.
    /// - **Gentle** v1 does not block. There is no pipeline to race: the
    ///   reconciler only cordons and waits, and the point of the mode is that
    ///   residents keep serving traffic normally until they leave on their
    ///   own. Blocking here would manufacture exactly the disruption the mode
    ///   exists to avoid, for a drain that may legitimately last days.
    /// - **v2**, draining or cancelling, does not block: a running sandbox's
    ///   calls go through while its node keeps it running, and what the
    ///   sealed node would refuse is [`Self::refuses_activations`].
    ///
    /// Two consequences of not blocking in gentle mode are intended. A wake of a
    /// suspended sandbox whose only snapshot is still local-only lands back on
    /// the draining node, adding a resident that has to leave again — accepted,
    /// because the alternative is 409ing a user's sandbox for the whole drain,
    /// and promotion keeps making those snapshots S3-backed, after which restore
    /// affinity sends them elsewhere. And a gentle drain an operator escalates
    /// to forced starts blocking from that moment, because the guards read the
    /// live mode rather than the mode at drain start.
    pub const fn blocks_placement(self) -> bool {
        match self {
            Self::V1 { phase, mode } => match phase {
                NodeDrainPhase::Draining => mode.blocks_user_calls_while_draining(),
                NodeDrainPhase::Drained | NodeDrainPhase::Terminating => true,
            },
            Self::V2 { phase } => match phase {
                V2DrainPhase::Draining | V2DrainPhase::Cancelling => false,
                V2DrainPhase::Drained | V2DrainPhase::Terminating => true,
            },
            Self::Unknown => true,
        }
    }

    /// Should a VM activation (a start, wake, restore, resume or runtime
    /// recovery) of a sandbox resolved to this node be refused? Wherever
    /// [`Self::blocks_placement`] says so, and in every v2 phase, its cancel
    /// included: a v2 drain's node is sealed and refuses every activation
    /// itself. [`Self::Unknown`] refuses, failing closed.
    ///
    /// The inverse reading matters too, and is why this is shared: anything whose
    /// safety depends on wakes being refused (the legacy sandbox-id-only snapshot
    /// reupload, which reads the live on-disk snapshot directory) may only run
    /// when this returns `true`.
    pub const fn refuses_activations(self) -> bool {
        match self {
            Self::V1 { .. } => self.blocks_placement(),
            Self::V2 { .. } | Self::Unknown => true,
        }
    }
}

impl fmt::Display for NodeDrain {
    /// The drain's state in a refusal's message: the phase, or what stands
    /// in for one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self, self.phase()) {
            (_, Some(phase)) => f.write_str(phase.as_str()),
            (Self::V2 { .. }, None) => f.write_str("being undrained"),
            _ => f.write_str("in a drain state this build doesn't know"),
        }
    }
}

/// How aggressively the node-drain reconciler empties a `draining` node.
///
/// Stored in `nodes.drain_mode`, chosen per drain by the operator through the
/// admin `:drain` request body. Orthogonal to `nodes.drain_phase`: a node in
/// either mode is cordoned, is excluded from placement, advances
/// `draining → drained` on the same zero-blockers gate, and terminates through
/// the same path. The mode governs only what the reconciler does to sandboxes
/// that are already resident, and whether the user-API drain gates reject
/// calls against them.
///
/// [`Self::Gentle`] is the answer everywhere a mode is missing or unreadable
/// rather than chosen — a `:drain` body with no `mode` (see
/// `pplx_space_admin_protocol::DrainRequest`), a NULL
/// `nodes.drain_mode` column, and a column value this build does not recognize
/// (the latter two via [`Self::from_column_str`]). That is one rule, and it is a safety
/// property rather than a preference: [`Self::Forced`] is the destructive mode,
/// so it is reachable only through an explicit, recognized `forced`. Every
/// other path degrades into "leave the residents alone", which stalls a drain
/// instead of suspending, evicting and restoring every sandbox on the node.
///
/// Because that rule is now uniform, [`Default`] states it once
/// ([`Self::Gentle`]) and both fallbacks defer to it. The leniency stops at the
/// column: [`Self::parse`] / [`FromStr`] / [`TryFrom<String>`] stay strict, so
/// a typo in a `:drain` body is a 400 rather than a silent gentle drain — a
/// rollout script that believes it asked for `forced` and got gentle would
/// cordon its nodes and wait forever while reporting success.
///
/// - [`Self::Forced`] — the original behavior. Every resident `started` /
///   `paused` sandbox is evacuated immediately (suspend → evict → restore
///   elsewhere), and the user-API drain gates return 409 on every
///   placement-affecting and access path to a resident sandbox for the whole
///   drain window, so users cannot race the evacuation pipeline. Per-node
///   disruption is proportional to resident live-sandbox count.
/// - [`Self::Gentle`] — no proactive evacuation. Residents keep serving
///   traffic and leave on their own (auto-suspend on inactivity, a user
///   suspend, or a delete). The local-snapshot promotion machinery still runs,
///   so local-only suspended artifacts become S3-backed and their restores
///   redirect off the node. Trades drain latency for zero user-visible
///   disruption; a node hosting a sandbox that never suspends never converges
///   on its own, which is what the admin `:drain` endpoint's in-place
///   escalation is for — a second `:drain` with `mode: forced` against a node
///   already `draining` rewrites this column without disturbing `drain_phase`
///   or `drain_started_at`, so the drain becomes destructive without losing
///   its age.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum NodeDrainMode {
    Forced,
    /// The default: see the type-level docs. A missing, NULL or unrecognized
    /// mode is gentle so the destructive branch needs an explicit token.
    #[default]
    Gentle,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported node drain mode '{0}'; expected 'forced' or 'gentle'")]
pub struct NodeDrainModeParseError(String);

impl NodeDrainMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Forced => NODE_DRAIN_MODE_FORCED,
            Self::Gentle => NODE_DRAIN_MODE_GENTLE,
        }
    }

    pub fn parse(value: &str) -> Result<Self, NodeDrainModeParseError> {
        match value {
            NODE_DRAIN_MODE_FORCED => Ok(Self::Forced),
            NODE_DRAIN_MODE_GENTLE => Ok(Self::Gentle),
            other => Err(NodeDrainModeParseError(other.to_string())),
        }
    }

    /// Does the reconciler proactively evacuate residents in this mode?
    pub const fn evacuates_residents(self) -> bool {
        matches!(self, Self::Forced)
    }

    /// Do the user-API drain gates reject placement-affecting and access
    /// calls against sandboxes resident on a node draining in this mode?
    ///
    /// Only meaningful for `drain_phase = 'draining'`: `drained` and
    /// `terminating` block regardless of mode, because the node is about to
    /// be destroyed.
    pub const fn blocks_user_calls_while_draining(self) -> bool {
        matches!(self, Self::Forced)
    }

    /// Interpret a raw `nodes.drain_mode` column value. **This is the only way
    /// drain-decision code should read that column.**
    ///
    /// NULL means no mode was recorded — the value on every node nobody has
    /// drained, and the value a future writer leaves if it forgets to set one.
    /// An unrecognized string means this build does not know the value someone
    /// wrote. Both read as [`Self::Gentle`], because neither is an operator
    /// asking to evacuate a node's sandboxes, and resolving either to `forced`
    /// would make the evacuating mode reachable by omission.
    ///
    /// An unrecognized value is deliberately *not* an error. Failing the read
    /// would propagate as a 500 from the user-API drain gate for every sandbox
    /// resolved to that node, turning one bad column value into an outage on
    /// healthy sandboxes. The column carries **no** `CHECK` constraint — the
    /// closed set is enforced here, in application code, so that adding a third
    /// mode costs one variant instead of a migration to widen the constraint
    /// plus an ordered follow-up code PR. That makes this branch load-bearing
    /// rather than defensive: any writer can put an unknown string in the
    /// column, including a newer service writing a mode an older reader has
    /// never heard of during a rollout.
    ///
    /// This resolver is **silent** — no logging. It sits on the per-request
    /// user-API drain gate, so warning here would emit one line per request for
    /// as long as one node carries a bad value, and the rollout that produces
    /// such a value is exactly when request volume is highest. Discoverability
    /// is the caller's job: [`Self::from_column_str_checked`] hands back the
    /// parse error, and the cluster controller's per-tick listing logs it once
    /// per node. The admin node listing resolves silently — it renders the
    /// effective mode, and a reader chasing an anomaly has the reconciler's warn.
    ///
    /// Takes `&str` rather than an already-decoded `Option<Self>` precisely so
    /// the unrecognized case is reachable: a typed `Option<NodeDrainMode>`
    /// column read would have failed in sqlx's `Decode` before this function
    /// ever saw it.
    pub fn from_column_str(value: Option<&str>) -> Self {
        Self::from_column_str_checked(value).unwrap_or_default()
    }

    /// [`Self::from_column_str`], but reporting an unrecognized value instead of
    /// absorbing it.
    ///
    /// Two entry points exist because the two readers want different things: the
    /// per-request drain gate wants the resolved mode and must stay silent, while
    /// the reconciler's per-tick listing wants to log what it could not parse. A
    /// caller that needs both gets them from one parse here rather than asking
    /// twice. NULL is not an error — it is the documented absence of a choice, so
    /// it resolves to [`Self::Gentle`] on the `Ok` path.
    pub fn from_column_str_checked(value: Option<&str>) -> Result<Self, NodeDrainModeParseError> {
        value.map_or(Ok(Self::Gentle), Self::parse)
    }
}

impl fmt::Display for NodeDrainMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for NodeDrainMode {
    type Err = NodeDrainModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl From<NodeDrainMode> for String {
    fn from(value: NodeDrainMode) -> Self {
        value.as_str().to_string()
    }
}

impl TryFrom<String> for NodeDrainMode {
    type Error = NodeDrainModeParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

#[cfg(feature = "sqlx")]
impl pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres> for NodeDrainMode {
    fn type_info() -> pplx_space_sql_codec::postgres::PgTypeInfo {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::type_info()
    }

    fn compatible(ty: &pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
        <str as pplx_space_sql_codec::Type<pplx_space_sql_codec::Postgres>>::compatible(ty)
    }
}

/// Writes only. There is deliberately **no** `Decode` impl: a typed column read
/// would fail the whole row on a value this build does not recognize, which is
/// exactly what [`NodeDrainMode::from_column_str`] exists to avoid. Reading the
/// column as `Option<String>` and resolving through that function is therefore
/// the only way, enforced here by the absence of the impl rather than by
/// convention.
#[cfg(feature = "sqlx")]
impl<'q> pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres> for NodeDrainMode {
    fn encode_by_ref(
        &self,
        buf: &mut pplx_space_sql_codec::postgres::PgArgumentBuffer,
    ) -> Result<pplx_space_sql_codec::encode::IsNull, pplx_space_sql_codec::error::BoxDynError>
    {
        <&str as pplx_space_sql_codec::Encode<'q, pplx_space_sql_codec::Postgres>>::encode(
            self.as_str(),
            buf,
        )
    }
}

#[cfg(feature = "serde")]
/// Optional JSON body for `POST /admin/api/v1/nodes/{id}:drain`.
///
/// `replace_after_terminate` controls whether the eventual ASG
/// `TerminateInstanceInAutoScalingGroup` call decrements the desired capacity:
///
/// - `true` (default) — preserve current single-node behavior: the ASG
///   auto-replaces the terminated instance.
/// - `false` — two-MNG blue-green rollout: the incumbent ASG must shrink
///   monotonically. Persisted on the node row so the terminator honors the
///   operator's original intent regardless of when the actual termination
///   call runs.
///
/// `mode` selects how aggressively the reconciler empties the node:
///
/// - `gentle` (default) — cordon and wait. Resident sandboxes keep serving
///   traffic and leave on their own (auto-suspend on inactivity, a user
///   suspend, or a delete); the user-API drain gates stay open for them.
/// - `forced` — evacuate every resident `started`/`paused` sandbox
///   immediately, and 409 every placement-affecting and access path to a
///   resident sandbox for the whole drain window. Per-node disruption is
///   proportional to resident live-sandbox count, so this is the opt-in.
///
/// Persisted on the node row, so the reconciler and the user-API gates both
/// read the operator's original intent rather than a process-wide setting.
///
/// `deny_unknown_fields` so a misspelled or camelCase key (e.g.
/// `replaceAfterTerminate`) surfaces as a 400 instead of silently falling
/// back to the default `true`. A rollout script that thinks it asked for
/// monotonic shrink and got auto-replace instead is the exact failure mode
/// this flag exists to avoid. The same reasoning covers `mode`: a typo'd
/// `"mode": "gentel"` is a 400, not a silent forced drain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainRequest {
    /// `None` means the request expressed no opinion, which is **not** the same
    /// as `Some(true)` even though both start a drain that auto-replaces.
    ///
    /// `:drain` has two edges, and the distinction is what keeps them both
    /// honest. On the edge that *starts* a drain, `None` resolves to the default
    /// `true` (see [`Self::replace_after_terminate_or_default`]). On a node
    /// already draining the intent is fixed — the terminator may act on it at
    /// any moment once the node reaches `drained`, so there is no window in
    /// which changing it has a defined meaning — and `None` is what lets an
    /// escalation say nothing about it. `Some(v)` disagreeing with the stored
    /// value is a 409 rather than a silent overwrite.
    ///
    /// A bare `bool` cannot express that. With no way to tell an omitted field
    /// from an explicit `true`, the endpoint has to choose between refusing
    /// every bodiless repeat against a shrinking node, silently applying a stale
    /// client's default (every `space-admin-cli` older than this field sends an
    /// explicit `!no_replace_after_terminate`), and silently dropping a
    /// deliberate correction. None of those surfaces as an error, which is
    /// exactly the case [`Option`] exists for.
    ///
    /// `skip_serializing_if` is load-bearing, not tidiness. `space-admin-cli` serializes
    /// this struct whole, and serde's `default` fires only on an **absent** key —
    /// an explicit `null` against an api-server that still types the field as
    /// `bool` is `invalid type: null, expected a boolean`, i.e. a 400 on every
    /// bodiless drain. CLI binaries roll independently of the api-server, so that
    /// skew window is real; omitting the key keeps the request byte-identical to
    /// what the previous release accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace_after_terminate: Option<bool>,
    /// `NodeDrainMode`'s own `Default` is gentle, and it is the single statement
    /// of "a missing mode is gentle" that the NULL-column read also defers to —
    /// so this does not restate the policy.
    #[serde(default)]
    pub mode: NodeDrainMode,
    /// `true` makes `:drain` a start-only request: it succeeds only against a
    /// node with no drain phase, and any existing drain is a 409 instead of
    /// being escalated or repeated. The check runs under the handler's row
    /// lock, so it is atomic with the write. That is what an orchestrator
    /// needs to start drains without ever escalating one somebody else
    /// started between its read and its POST.
    ///
    /// `false` (the default) keeps the escalate-in-place and same-mode edges.
    /// Skipped when `false`, so a request that doesn't ask for it stays
    /// byte-identical to what the previous release accepts. One that does ask
    /// is a 400 from an api-server that predates the field (this struct is
    /// `deny_unknown_fields`), which fails closed.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_undrained: bool,
}

#[cfg(feature = "serde")]
impl DrainRequest {
    /// The replacement intent to persist when *starting* a drain: an unexpressed
    /// opinion means the ASG auto-replaces the terminated instance, which
    /// preserves single-node-drain semantics.
    pub fn replace_after_terminate_or_default(&self) -> bool {
        self.replace_after_terminate.unwrap_or(true)
    }
}

#[cfg(test)]
mod tests;
