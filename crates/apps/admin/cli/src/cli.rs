use clap::builder::TypedValueParser;
use clap::{Parser, Subcommand, ValueEnum};
use pplx_space_common_core::types::EnvironmentReference;
use pplx_space_common_core::types::admin::AdminNodeSort;
use std::str::FromStr;

/// Operator-facing SPACE CLI. The current command set is a thin wrapper over
/// the `/admin/api/v1/...` surface (node drain / terminate / list); more SPACE
/// subcommands can grow under the same binary over time.
#[derive(Parser)]
#[command(
    name = "space-admin-cli",
    about = "SPACE operator CLI",
    version,
    disable_version_flag = true
)]
pub(crate) struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    pub(crate) _version: (),

    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Log in using SPACE_API_URL and verify admin access.
    Login(LoginArgs),
    /// Start draining a node (POST /admin/api/v1/nodes/{node}:drain)
    #[command(name = "drain-node")]
    Drain {
        /// Node UUID
        node: String,
        /// Tell the cluster controller NOT to ask the ASG to auto-replace
        /// this instance on termination. Sets `replace_after_terminate=false`
        /// in the request body, which persists `terminate_decrement_capacity=true`
        /// on the node row. Use for two-MNG blue-green rollouts where the
        /// incumbent ASG must shrink monotonically. Default behavior
        /// (flag omitted) keeps single-node-drain semantics: the ASG
        /// auto-replaces the terminated instance.
        ///
        /// Only honoured on the call that *starts* a drain. A drain's
        /// replacement intent is fixed for its lifetime, so passing this against
        /// a node that is already draining is a 409 if it disagrees with what
        /// that drain was started with — omit it to escalate on the existing
        /// terms, or `undrain-node` and start a fresh drain to change them.
        #[arg(long)]
        no_replace_after_terminate: bool,
        /// Evacuate resident sandboxes immediately instead of waiting for them
        /// to leave on their own. Sets `mode=forced` in the request body.
        ///
        /// The default drain is gentle: the node is cordoned, resident
        /// sandboxes keep serving traffic, and the node advances to `drained`
        /// once they have all suspended on inactivity, been suspended by their
        /// user, or been deleted. Forced instead suspends and migrates every
        /// resident `started`/`paused` sandbox right away and 409s user calls
        /// against them for the whole drain window — faster, but per-node
        /// disruption is proportional to how many live sandboxes it hosts.
        ///
        /// Also escalates a drain already in flight. Re-running this against a
        /// node that is already draining gently switches it to forced in place:
        /// the node stays cordoned throughout, and the drain keeps the start
        /// time it already had. Use this rather than `undrain-node` +
        /// `drain-node --forced`, which uncordons the node between the two
        /// calls and lets fresh sandboxes land on it. Downgrading the other way
        /// (`drain-node` without `--forced`, against a forced drain) is refused
        /// — abort with `undrain-node` instead.
        ///
        /// Escalating also re-enables the stale-transient sweep, which is
        /// forced-only. Any resident wedged in a non-evacuable transient
        /// lifecycle (creating, starting, resuming, and similar) past the
        /// cluster's abandon age is flipped to `failed`, unrecoverably.
        ///
        /// There is no grace period. The age is measured from the sandbox's own
        /// last lifecycle change, not from the escalation, so a resident that
        /// has been wedged for longer than the abandon age while the node
        /// drained gently is already past it and is flipped on the **first**
        /// tick after the write.
        ///
        /// Before escalating a drain that has been sitting, read **both**
        /// `node_drain.sandboxes_remaining{node}` and
        /// `node_drain.drain_blockers{node}`. Neither one is a superset of what
        /// the sweep destroys — they differ in both the lifecycles and the
        /// node-pinning column they count — so two zeros is the only
        /// reassuring answer, and a non-zero in either is a prompt to look at
        /// the rows rather than proof something will be lost.
        #[arg(long)]
        forced: bool,
    },
    /// Terminate a drained node (POST /admin/api/v1/nodes/{node}:terminate)
    #[command(name = "terminate-node")]
    Terminate {
        /// Node UUID
        node: String,
    },
    /// Cancel an in-flight drain (POST /admin/api/v1/nodes/{node}:undrain)
    #[command(name = "undrain-node")]
    Undrain {
        /// Node UUID
        node: String,
    },
    /// List nodes (GET /admin/api/v1/nodes), optionally filtered by drain phase or role
    #[command(name = "list-nodes")]
    ListNodes(ListNodesArgs),
    /// Show a single node (GET /admin/api/v1/nodes/{node})
    #[command(name = "get-node")]
    GetNode {
        /// Node UUID
        node: String,
        /// Compute occupancy and drainability diagnostics (may be slow).
        #[arg(long)]
        diagnostics: bool,
        /// Emit the raw JSON response instead of a formatted view.
        #[arg(long)]
        json: bool,
    },
    /// Manage cluster warm-pool configs.
    #[command(name = "warm-pool")]
    WarmPool {
        #[command(subcommand)]
        command: WarmPoolCommands,
    },
    /// Build SPACE system templates through the Admin API.
    Template {
        #[command(subcommand)]
        command: TemplateCommands,
    },
    /// Manage the cluster's execution environments.
    Runtime {
        #[command(subcommand)]
        command: RuntimeCommands,
    },
}

#[derive(Subcommand)]
pub(crate) enum RuntimeCommands {
    /// Read and set the execution environment policy
    /// (/admin/api/v1/runtime-environments).
    Env {
        #[command(subcommand)]
        command: RuntimeEnvCommands,
    },
}

#[derive(Subcommand)]
pub(crate) enum RuntimeEnvCommands {
    /// Show the policy: generation, default, and each environment's state.
    List {
        /// Emit the raw JSON response instead of a formatted view.
        #[arg(long)]
        json: bool,
    },
    /// Make an environment the cluster default. It becomes allowed if it
    /// isn't, and the previous default stays allowed. The first default also
    /// stops every environment with no state, so mark the ones to keep first.
    Default {
        environment: EnvironmentReference,
        /// Why, recorded with you on the policy.
        #[arg(long)]
        reason: String,
        /// Set the cluster's first default even though work may depend on
        /// an environment it stops (one with no state, or one whose state
        /// doesn't execute), or on a sandbox or in-use template release
        /// whose environment can't be told.
        /// Without it, such a write is refused and the counts are shown.
        /// Later defaults stop nothing.
        #[arg(long)]
        strand: bool,
        /// Apply only if the policy is still at this generation (the one
        /// `runtime env list` showed), so a write from a stale read is
        /// refused instead of undoing someone else's change.
        #[arg(long, value_name = "GENERATION")]
        if_generation: Option<u64>,
        /// Emit the raw JSON response.
        #[arg(long)]
        json: bool,
    },
    /// Set an environment's state. The default must stay allowed.
    State {
        environment: EnvironmentReference,
        state: RuntimeEnvironmentStateArg,
        /// Why, recorded with you on the policy.
        #[arg(long)]
        reason: String,
        /// Block even though work may still depend on the environment:
        /// sandboxes or in-use template releases bound to it, or ones whose
        /// environment can't be told (no binding, an unreadable one, or no
        /// record). Without it, such a block is refused and the counts are
        /// shown. A block of an environment that already doesn't execute
        /// needs none.
        #[arg(long)]
        strand: bool,
        /// Apply only if the policy is still at this generation (the one
        /// `runtime env list` showed), so a write from a stale read is
        /// refused instead of undoing someone else's change.
        #[arg(long, value_name = "GENERATION")]
        if_generation: Option<u64>,
        /// Emit the raw JSON response.
        #[arg(long)]
        json: bool,
    },
    /// Show each node's enforced policy and whether it matches the cluster's.
    Nodes {
        /// Start after this node id (the previous page's next_after).
        #[arg(long, conflicts_with = "all")]
        after: Option<uuid::Uuid>,
        /// Follow next_after through every page.
        #[arg(long)]
        all: bool,
        /// Emit the raw JSON response; with --all, a JSON array of pages.
        #[arg(long)]
        json: bool,
    },
    /// Count sandboxes and template releases per environment.
    Population {
        /// Emit the raw JSON response instead of a formatted table.
        #[arg(long)]
        json: bool,
    },
}

/// The states `PUT .../{environment}/state` accepts. The server owns the
/// vocabulary and rejects anything else; this copy lists them in `--help` and
/// refuses a misspelled one before anything is sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum RuntimeEnvironmentStateArg {
    Allowed,
    Deprecated,
    Blocked,
}

impl RuntimeEnvironmentStateArg {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Deprecated => "deprecated",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(clap::Args)]
pub(crate) struct LoginArgs {
    /// Print only the verified session token to stdout.
    #[arg(long)]
    pub(crate) print_token: bool,
    /// Admin API origin.
    #[arg(long, env = "SPACE_API_URL")]
    pub(crate) api_url: String,
    /// Authentication origin when it differs from the admin API.
    #[arg(long)]
    pub(crate) auth_url: Option<String>,
    /// Account email (prompted when omitted).
    #[arg(long)]
    pub(crate) email: Option<String>,
    /// Read the password from stdin instead of the terminal.
    #[arg(long, requires = "email")]
    pub(crate) password_stdin: bool,
}

#[derive(Subcommand)]
pub(crate) enum TemplateCommands {
    /// Build the deployed seed or code-runtime recipe and wait for publication.
    Build {
        template: pplx_space_common_core::types::SystemTemplate,
        /// Seed reference for code-runtime (default: seed:latest).
        #[arg(long)]
        base: Option<pplx_space_common_core::types::TemplateReference>,
        /// Assign a release tag, such as latest, after a successful build.
        #[arg(long = "publish-tag")]
        publish_tags: Vec<pplx_space_common_core::types::TemplateTag>,
        /// Return the job ID immediately.
        #[arg(long = "async")]
        async_mode: bool,
    },
    /// Inspect a submitted build, including publication failures.
    Status {
        template: pplx_space_common_core::types::SystemTemplate,
        job_id: pplx_space_common_core::types::BuildRunId,
    },
}

/// Accepted `--sort` values, taken from [`AdminNodeSort::ALL`] rather than
/// written out again. Two things follow from that, both of which a hand-written
/// `value_name` gave up: `--help` and shell completion list the real set, and a
/// variant added to the shared enum shows up here without a second edit — the
/// shared type's own docs give single-sourcing as the reason its parse error is
/// rendered from `ALL`, and a CLI placeholder that mirrored the set by hand
/// reintroduced exactly the drift that avoids.
///
/// `PossibleValuesParser` screens the value before `FromStr` sees it, so the
/// `try_map` only ever re-derives a variant clap has already accepted; the
/// vocabulary is still the shared enum's, not a local copy.
fn sort_parser() -> impl clap::builder::TypedValueParser<Value = AdminNodeSort> {
    clap::builder::PossibleValuesParser::new(
        AdminNodeSort::ALL
            .iter()
            .map(|sort| sort.as_str())
            .collect::<Vec<_>>(),
    )
    .try_map(|value: String| AdminNodeSort::from_str(&value))
}

#[derive(clap::Args)]
pub(crate) struct ListNodesArgs {
    /// Only show nodes in this drain phase. `none` matches nodes with no
    /// drain in progress (`drain_phase IS NULL` server-side).
    #[arg(long, value_enum)]
    pub(crate) drain_phase: Option<DrainPhaseFilter>,
    /// Only show nodes with this role. Required with `--sort drainability`:
    /// the ranking has no role term, and builders and cluster nodes host no
    /// sandboxes, so they win every count and would lead an unfiltered ranked
    /// listing — the first "best candidate" offered would be a builder. Ranking
    /// is only meaningful within one role, so the CLI makes the caller name it
    /// rather than leaving a misread one keystroke away.
    // `required_if_eq` compares clap's *raw* argument token, not the parsed
    // `AdminNodeSort`, so this literal has to stay equal to
    // `AdminNodeSort::Drainability.as_str()` — and any future variant whose
    // `needs_drainability()` is true needs its own arm. That coupling is
    // invisible to the compiler, so
    // `list_nodes_requires_a_role_for_every_ranking_variant` walks
    // `AdminNodeSort::ALL` and fails if a variant escapes it.
    #[arg(long, value_enum, required_if_eq("sort", "drainability"))]
    pub(crate) role: Option<RoleFilter>,
    /// Include rows soft-deleted from `node_direct`.
    #[arg(long)]
    pub(crate) include_deleted: bool,
    /// Ordering: `name` (default) or `drainability`. The latter ranks the
    /// nodes whose residents will actually leave first, and adds the VISIBLE,
    /// LIVE SBX, LONG RUNNERS, GATE BLOCKERS and TERM BLOCKERS columns.
    /// Whether those counts discount sandbox rows the node itself no longer
    /// reports depends on the cluster's residency policy — `space-admin-cli get-node --diagnostics`
    /// prints it as `Residency Policy`, and under the `lifecycle_only` default
    /// nothing is discounted, so the counts include phantom rows.
    #[arg(long, value_parser = sort_parser())]
    pub(crate) sort: Option<AdminNodeSort>,
    /// Age past which a live sandbox counts as a long-runner, in hours.
    /// Left unset, the server picks the threshold. Only meaningful with
    /// `--sort drainability`.
    #[arg(long)]
    pub(crate) long_runner_age_hours: Option<u32>,
    /// Add extra columns: substrate k8s node name, inventory state,
    /// and any drain reconciler error.
    #[arg(long, short = 'v')]
    pub(crate) verbose: bool,
    /// Emit the raw JSON response instead of a formatted table.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Subcommand)]
pub(crate) enum WarmPoolCommands {
    /// Create or update a cluster warm-pool config.
    #[command(name = "create", visible_alias = "add")]
    Create(WarmPoolConfigArgs),
    /// Update an exact cluster warm-pool config.
    #[command(name = "update")]
    Update(WarmPoolUpdateArgs),
    /// List cluster warm-pool configs.
    #[command(name = "list")]
    List {
        /// Emit the raw JSON response instead of a formatted table.
        #[arg(long)]
        json: bool,
    },
    /// Show a warm-pool config by UUID prefix or template reference.
    #[command(name = "get")]
    Get {
        /// Pool UUID/prefix or template reference, e.g. my-template or my-template@sha256:...
        pool: String,
        /// Emit the raw JSON response instead of a formatted view.
        #[arg(long)]
        json: bool,
    },
}

#[derive(clap::Args)]
pub(crate) struct WarmPoolConfigArgs {
    /// Template name, e.g. my-template.
    #[arg(long)]
    pub(crate) template: String,
    /// Exact template release content hash. Omit to use the template's latest tag.
    #[arg(long)]
    pub(crate) content_hash: Option<String>,
    /// Sandbox vCPU count for slots in this pool.
    #[arg(long)]
    pub(crate) vcpu: u32,
    /// Sandbox memory in MiB for slots in this pool.
    #[arg(long)]
    pub(crate) memory_mib: u32,
    /// Desired number of ready slots.
    #[arg(long)]
    pub(crate) target_ready_slots: u32,
    /// Maximum new warming slots created per reconciler tick.
    #[arg(long)]
    pub(crate) refill_per_tick: u32,
    /// Maximum concurrent warming slots for this pool.
    #[arg(long)]
    pub(crate) max_warming_slots: u32,
    /// Create/update the config as disabled.
    #[arg(long)]
    pub(crate) disabled: bool,
    /// Emit the raw JSON response instead of a formatted view.
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(clap::Args)]
pub(crate) struct WarmPoolUpdateArgs {
    /// Exact template reference, e.g. my-template@sha256:...
    #[arg(long)]
    pub(crate) template: String,
    /// Sandbox vCPU count for slots in this pool.
    #[arg(long)]
    pub(crate) vcpu: u32,
    /// Sandbox memory in MiB for slots in this pool.
    #[arg(long)]
    pub(crate) memory_mib: u32,
    /// Desired number of ready slots.
    #[arg(long)]
    pub(crate) target_ready_slots: u32,
    /// Maximum new warming slots created per reconciler tick.
    #[arg(long)]
    pub(crate) refill_per_tick: u32,
    /// Maximum concurrent warming slots for this pool.
    #[arg(long)]
    pub(crate) max_warming_slots: u32,
    /// Update the config as disabled.
    #[arg(long)]
    pub(crate) disabled: bool,
    /// Emit the raw JSON response instead of a formatted view.
    #[arg(long)]
    pub(crate) json: bool,
}

/// Mirrors the values accepted by the admin API's `drain_phase` query
/// parameter (see `ListAdminNodesFilters` in
/// `crates/libs/api/src/routes/admin/node_store.rs`).
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum DrainPhaseFilter {
    Draining,
    Drained,
    Terminating,
    /// `drain_phase IS NULL` server-side (no drain in progress).
    None,
}

impl DrainPhaseFilter {
    pub(crate) fn as_query_value(self) -> &'static str {
        match self {
            Self::Draining => "draining",
            Self::Drained => "drained",
            Self::Terminating => "terminating",
            Self::None => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum RoleFilter {
    Worker,
    Builder,
    Cluster,
}

impl RoleFilter {
    pub(crate) fn as_query_value(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Builder => "builder",
            Self::Cluster => "cluster",
        }
    }
}

#[cfg(test)]
mod tests;

/// Connection settings read from the environment so the CLI matches the
/// existing `scripts/tests/smoke-spaced-rest.sh` interface operators already
/// use.
pub(crate) struct EnvConfig {
    pub(crate) api_url: String,
    pub(crate) api_token: String,
}

impl EnvConfig {
    pub(crate) fn from_env() -> Result<Self, String> {
        let api_url = std::env::var("SPACE_API_URL").map_err(|error| {
            format!("failed to read SPACE_API_URL (e.g. http://127.0.0.1:8080): {error}")
        })?;
        let api_token = std::env::var("SPACE_API_TOKEN")
            .map_err(|error| format!("failed to read SPACE_API_TOKEN: {error}"))?;
        if api_url.trim().is_empty() {
            return Err("SPACE_API_URL must not be empty".to_string());
        }
        if api_token.trim().is_empty() {
            return Err("SPACE_API_TOKEN must not be empty".to_string());
        }
        Ok(Self { api_url, api_token })
    }
}
