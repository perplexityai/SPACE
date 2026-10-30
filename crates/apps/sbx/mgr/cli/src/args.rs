use super::*;

#[derive(Parser)]
#[command(
    name = "space-sbxmgr",
    about = "Manage sandboxes",
    version,
    disable_version_flag = true
)]
pub(super) struct Cli {
    /// Print version.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::Version)]
    pub(super) _version: (),

    #[command(subcommand)]
    pub(super) command: Commands,
}

pub(super) fn parse_env_vars(values: Vec<String>) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    for value in values {
        let Some((key, env_value)) = value.split_once('=') else {
            exit_error(format!("--env must be KEY=VALUE, got '{value}'"));
        };
        if env.insert(key.to_string(), env_value.to_string()).is_some() {
            exit_error(format!("duplicate env key '{key}'"));
        }
    }
    let (env, issues) = pplx_space_common_core::types::EnvMap::sanitize(env);
    for issue in issues {
        eprintln!("WARNING: env sanitized: {issue:?}");
    }
    env.into()
}

#[derive(Debug, Clone, Args, Default)]
pub(super) struct AutoLifecycleArgs {
    /// Explicitly disable auto lifecycle for this sandbox or template.
    #[arg(long, conflicts_with_all = ["wake_on", "idle_policy", "idle_break_on", "max_guest_idle_extension"])]
    auto_lifecycle_off: bool,
    /// Wake sources: access, command. Repeat or pass a comma-separated list.
    #[arg(long = "wake-on", value_delimiter = ',', num_args = 1..)]
    wake_on: Vec<AutoLifecycleWakeSource>,
    /// Idle stages, e.g. "pause@5m,suspend@15m,stop@45m".
    #[arg(long = "idle-policy")]
    idle_policy: Option<AutoLifecycleStageChain>,
    /// Which events break the idle timer: access, command, guest-activity.
    #[arg(long = "idle-break-on", value_delimiter = ',', num_args = 1..)]
    idle_break_on: Vec<AutoLifecycleIdleBreakSource>,
    /// Maximum extension a guest-originated active hint may request.
    #[arg(long)]
    max_guest_idle_extension: Option<SimpleDuration>,
}

impl AutoLifecycleArgs {
    pub(super) fn is_explicit(&self) -> bool {
        self.auto_lifecycle_off
            || !self.wake_on.is_empty()
            || self.idle_policy.is_some()
            || !self.idle_break_on.is_empty()
            || self.max_guest_idle_extension.is_some()
    }

    pub(super) fn normalized_idle_break_on(&self) -> Vec<AutoLifecycleIdleBreakSource> {
        let mut sources = self.idle_break_on.clone();
        sources.sort_unstable_by_key(|source| match source {
            AutoLifecycleIdleBreakSource::Access => 0u8,
            AutoLifecycleIdleBreakSource::Command => 1u8,
            AutoLifecycleIdleBreakSource::GuestActivity => 2u8,
        });
        sources.dedup();
        if sources.contains(&AutoLifecycleIdleBreakSource::Access) {
            sources.retain(|source| *source != AutoLifecycleIdleBreakSource::Command);
        }
        if self.idle_policy.is_some() && sources.is_empty() {
            vec![AutoLifecycleIdleBreakSource::Access]
        } else {
            sources
        }
    }

    pub(super) fn to_policy(&self) -> Result<Option<AutoLifecyclePolicy>, String> {
        if !self.is_explicit() {
            return Ok(None);
        }
        if self.auto_lifecycle_off {
            return Ok(Some(AutoLifecyclePolicy::disabled()));
        }
        if !self.idle_break_on.is_empty() && self.idle_policy.is_none() {
            return Err("--idle-break-on requires --idle-policy".to_string());
        }

        let idle_break_on = self.normalized_idle_break_on();
        if self.max_guest_idle_extension.is_some()
            && !idle_break_on.contains(&AutoLifecycleIdleBreakSource::GuestActivity)
        {
            return Err(
                "--max-guest-idle-extension requires guest-activity in --idle-break-on".to_string(),
            );
        }

        let mut policy = AutoLifecyclePolicy {
            wake: AutoLifecycleWakePolicy {
                on: self.wake_on.clone(),
            },
            condition_sets: BTreeMap::new(),
            stages: Vec::new(),
        };

        if let Some(chain) = self.idle_policy.as_ref() {
            if idle_break_on.contains(&AutoLifecycleIdleBreakSource::Access) {
                append_condition_stages(
                    &mut policy,
                    AutoLifecycleConditionSetId::AccessIdle,
                    AutoLifecycleConditionSet::access_idle(),
                    chain,
                )?;
            } else if idle_break_on.contains(&AutoLifecycleIdleBreakSource::Command) {
                append_condition_stages(
                    &mut policy,
                    AutoLifecycleConditionSetId::CommandIdle,
                    AutoLifecycleConditionSet::command_idle(),
                    chain,
                )?;
            }

            if idle_break_on.contains(&AutoLifecycleIdleBreakSource::GuestActivity) {
                append_condition_stages(
                    &mut policy,
                    AutoLifecycleConditionSetId::GuestActivity,
                    AutoLifecycleConditionSet::guest_activity(self.max_guest_idle_extension),
                    chain,
                )?;
            }
        }

        let policy = policy.normalized();
        policy.validate_basic()?;
        Ok(Some(policy))
    }
}

pub(super) fn append_condition_stages(
    policy: &mut AutoLifecyclePolicy,
    condition_name: AutoLifecycleConditionSetId,
    condition: AutoLifecycleConditionSet,
    chain: &AutoLifecycleStageChain,
) -> Result<(), String> {
    if chain.is_empty() {
        return Err(format!(
            "{condition_name} must include at least one stage when specified"
        ));
    }
    policy.condition_sets.insert(condition_name, condition);
    policy
        .stages
        .extend(chain.clone().into_condition_stages(condition_name));
    Ok(())
}

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(super) enum NetworkPolicyMode {
    Open,
    Close,
}

/// CLI surface for `SuspendSnapshotFormat`. Mirrors the proto enum's
/// two real variants; `Unspecified` is unreachable from the CLI because
/// every invocation gets a default (`Full`).
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(super) enum SuspendFormatArg {
    Full,
    BtrfsSend,
}

impl SuspendFormatArg {
    pub(super) fn to_proto(self) -> SuspendSnapshotFormat {
        match self {
            Self::Full => SuspendSnapshotFormat::Full,
            Self::BtrfsSend => SuspendSnapshotFormat::BtrfsSend,
        }
    }
}

impl NetworkPolicyMode {
    pub(super) fn to_proto(self) -> NetworkPolicyProto {
        let action = match self {
            Self::Open => NetworkPolicyAction::Allow,
            Self::Close => NetworkPolicyAction::Deny,
        };
        NetworkPolicyProto {
            version: NetworkPolicyVersion::V1.into(),
            egress_default_action: action.into(),
            ..Default::default()
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Close => "close",
        }
    }
}

pub(super) fn network_policy_mode_from_proto(
    policy: &NetworkPolicyProto,
) -> Result<NetworkPolicyMode, String> {
    let action = NetworkPolicyAction::try_from(policy.egress_default_action)
        .unwrap_or(NetworkPolicyAction::Unspecified);
    match action {
        NetworkPolicyAction::Allow => Ok(NetworkPolicyMode::Open),
        NetworkPolicyAction::Deny => Ok(NetworkPolicyMode::Close),
        NetworkPolicyAction::Unspecified => {
            Err("network policy has unspecified egress default action".to_string())
        }
    }
}

pub(super) fn print_network_policy(mode: NetworkPolicyMode, source: Option<&str>) {
    println!("policy: {}", mode.as_str());
    if let Some(source) = source {
        println!("source: {source}");
    }
}

#[derive(clap::Subcommand)]
pub(super) enum Commands {
    /// Create a new sandbox (copies rootfs, optionally starts it)
    Create {
        /// Exact published template content digest.
        #[arg(long)]
        content_hash: Option<pplx_space_common_core::types::ContentHash>,
        /// Exact environment; must match the template's immutable binding.
        #[arg(long)]
        environment: Option<pplx_space_common_core::types::EnvironmentReference>,
        /// Legacy runtime selector; an environment determines its own runtime.
        #[arg(
            long,
            required_unless_present = "environment",
            conflicts_with = "environment"
        )]
        runtime: Option<String>,
        #[arg(long)]
        template: String,
        /// Local operator sandbox owner label
        #[arg(long)]
        user: Option<String>,
        /// Enable rolling btrfs snapshots when started
        #[arg(long)]
        snapshots: bool,
        /// Start the sandbox immediately after creation
        #[arg(long)]
        autostart: bool,
        /// Boot mode: cold, base-snapshot, pool (fallthrough: pool → base-snapshot → cold)
        #[arg(long, default_value = "cold")]
        boot_mode: String,
        /// Block until guest reports ready (only with --autostart)
        #[arg(long)]
        blocking: bool,
        /// Sandbox env var for SPACE-launched processes and space-ssh sessions (KEY=VALUE)
        #[arg(long = "env")]
        env: Vec<String>,
        /// Explicit sandbox rootfs size in MiB. If unset, use the template rootfs as-is.
        /// Requires cold creation: --boot-mode base-snapshot is rejected;
        /// --boot-mode pool falls back to cold.
        #[arg(long)]
        rootfs_size_mb: Option<u32>,
        #[command(flatten)]
        auto_lifecycle: AutoLifecycleArgs,
    },
    /// Start an existing sandbox (fresh boot or restore from checkpoint)
    Start {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Restore from checkpoint or "suspend" state instead of fresh boot
        #[arg(long)]
        checkpoint: Option<String>,
        /// Guest memory in MiB for this start
        #[arg(long)]
        memory: Option<u32>,
        /// Start from base memory snapshot instead of cold boot
        #[arg(long)]
        from_warm: bool,
        /// Block until guest reports ready (or timeout)
        #[arg(long)]
        blocking: bool,
        /// Ready timeout in seconds for --blocking
        #[arg(long)]
        ready_timeout_secs: Option<u32>,
    },
    /// Send a signal to a sandbox and cleanup (default: SIGKILL)
    Kill {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Signal number (default: 9 = SIGKILL)
        #[arg(long, default_value = "9")]
        signal: u32,
        /// For fatal signals, wait until cleanup reaches stopped
        #[arg(long)]
        blocking: bool,
    },
    /// Stop a running sandbox (keeps rootfs)
    Stop {
        /// Sandbox ID, long UUID prefix, or short label
        id: Option<SandboxSelector>,
        /// Stop all sandboxes
        #[arg(long)]
        all: bool,
        #[arg(long)]
        template: Option<String>,
        /// Wait until cleanup reaches stopped
        #[arg(long)]
        blocking: bool,
        /// Skip SIGTERM grace and use SIGKILL immediately
        #[arg(long)]
        immediate_kill: bool,
    },
    /// Delete a sandbox (stops it first if running, removes rootfs)
    Delete {
        /// Sandbox ID, long UUID prefix, or short label
        id: Option<SandboxSelector>,
        /// Delete all sandboxes
        #[arg(long)]
        all: bool,
        #[arg(long)]
        template: Option<String>,
        /// Wait until matching rows are deleted
        #[arg(long)]
        blocking: bool,
    },
    /// List all sandboxes
    List,
    /// Clean up dead sandboxes (mark as failed)
    Cleanup,
    /// Show per-sandbox resource stats (network, cgroup CPU/memory/PIDs)
    Stats {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Show cached component health and the last five background observations
    Health {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Show disk usage from the last completed node disk census
    DiskUsage {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Execute a command inside a running sandbox
    Exec {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Execute the command as this guest user. Omit to use the guest default.
        #[arg(long)]
        user: Option<GuestUserName>,
        /// Command and arguments
        #[arg(trailing_var_arg = true, required = true)]
        command: Vec<String>,
    },
    /// Update resource limits for a running sandbox
    Update {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// vCPU count
        #[arg(long)]
        vcpu: Option<u32>,
        /// Memory in MiB
        #[arg(long)]
        memory: Option<u32>,
        /// Max PIDs
        #[arg(long)]
        pids: Option<u32>,
        /// Reapply the template's default auto lifecycle to this sandbox.
        #[arg(long)]
        inherit_template_auto_lifecycle: bool,
        #[command(flatten)]
        auto_lifecycle: AutoLifecycleArgs,
    },
    /// List available templates
    Templates,
    /// Update a template's default auto lifecycle policy
    TemplateSetAutoLifecycle {
        /// Template name
        template: String,
        #[command(flatten)]
        auto_lifecycle: AutoLifecycleArgs,
    },
    /// Get or update a sandbox network policy.
    NetworkPolicy {
        /// Sandbox ID, long UUID prefix, or short label.
        sandbox_id: SandboxSelector,
        /// Set policy to open or close.
        #[arg(long, conflicts_with = "clear")]
        set: Option<NetworkPolicyMode>,
        /// Clear the custom policy and inherit this sandbox's template policy.
        #[arg(long, conflicts_with = "set")]
        clear: bool,
    },
    /// Show the live net-gateway policy hydration state.
    NetworkState {
        /// Sandbox ID, long UUID prefix, or short label.
        sandbox_id: SandboxSelector,
    },
    /// Toggle verbose net-gateway debug logging for a sandbox.
    ///
    /// Pass `--duration` to enable a self-expiring debug window on the live
    /// gateway so its sniffed-SNI logs become visible, or `--disable` to turn it
    /// off immediately. The CLI sends an absolute deadline (`now + duration`); the
    /// gateway clamps it and clears the flag on its own and on restart, so it
    /// never becomes a standing production log stream.
    NetworkDebug {
        /// Sandbox ID, long UUID prefix, or short label.
        sandbox_id: SandboxSelector,
        /// How long debug logging should stay enabled, e.g. 5m or 30s.
        /// Required unless `--disable` is given.
        #[arg(long, required_unless_present = "disable", conflicts_with = "disable")]
        duration: Option<SimpleDuration>,
        /// Disable net-gateway debug logging immediately.
        #[arg(long)]
        disable: bool,
    },
    /// Cycle (kill + relaunch in place) net-gateway sidecars so they run the
    /// node's current binary. The Firecracker VM keeps running; in-flight
    /// guest connections through the old gateway drop and policy re-hydrates
    /// via the queued reconcile.
    ///
    /// Dry-run by default: prints the selection and exits. Pass `--execute`
    /// to actually cycle. Cycles run sequentially with `--sleep` between
    /// them and stop at `--max`; Ctrl-C aborts between cycles.
    CycleNetGateway {
        /// Sandbox IDs, long UUID prefixes, or short labels to cycle.
        /// Mutually exclusive with `--stale` and `--refresh-config`.
        #[arg(required_unless_present_any = ["stale", "refresh_config"], conflicts_with_all = ["stale", "refresh_config"])]
        sandbox_ids: Vec<SandboxSelector>,
        /// Select gateways running a stale binary.
        #[arg(long)]
        stale: bool,
        /// Select every live gateway, even at the current SHA, to reread boot
        /// configuration. Repeating this selection cycles the same gateways
        /// again; this is not a configuration-drift detector.
        #[arg(long, conflicts_with = "stale")]
        refresh_config: bool,
        /// Print only selected full sandbox UUIDs, one per line, for a saved cohort.
        #[arg(long, conflicts_with = "execute")]
        ids_only: bool,
        /// Actually cycle the selected gateways. Without this flag the
        /// command only prints what would be cycled.
        #[arg(long)]
        execute: bool,
        /// Maximum number of gateways to cycle in one invocation.
        #[arg(long, default_value_t = 5)]
        max: usize,
        /// Pause between consecutive cycles in SimpleDuration syntax
        /// (decimal + s/m/h/d), e.g. 2s or 0.5s. "500ms" is not accepted.
        #[arg(long, default_value = "2s")]
        sleep: SimpleDuration,
        /// Skip sandboxes created more recently than this (e.g. 30m, 2h),
        /// to avoid churning gateways that were launched mid-rollout or
        /// whose staleness reading may not have refreshed yet.
        #[arg(long)]
        min_age: Option<SimpleDuration>,
    },
    /// Pause a running sandbox (freeze vCPUs, process stays alive)
    Pause {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Compatibility flag; pause always waits until state is paused
        #[arg(long)]
        blocking: bool,
    },
    /// Resume a paused sandbox (thaw vCPUs)
    Resume {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Block until guest health check passes (or timeout)
        #[arg(long)]
        blocking: bool,
        /// Ready timeout in seconds for --blocking
        #[arg(long)]
        ready_timeout_secs: Option<u32>,
    },
    /// Suspend a sandbox (snapshot + stop, frees resources)
    Suspend {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Wait until state is suspended
        #[arg(long)]
        blocking: bool,
        /// Rootfs codec for the snapshot. `full` (the default) writes the
        /// entire `rootfs.ext4` image — the only codec used by any pre-PR
        /// snapshot in production, and the safe choice for any sandbox
        /// whose `instance` was created via the legacy `create + reflink`
        /// path. `btrfs-send` writes a `btrfs send -p <template>
        /// <instance>` stream and requires the instance to be a
        /// subvolume snapshot of the local template; the suspend handler
        /// rejects the request with `FailedPrecondition` when that
        /// lineage isn't present so the caller can retry with `full`.
        #[arg(long, value_enum, default_value_t = SuspendFormatArg::Full)]
        format: SuspendFormatArg,
    },
    /// Bring a sandbox back to life using resume/restore/start as needed
    Wake {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Block until guest is ready for access
        #[arg(long)]
        blocking: bool,
    },
    /// Checkpoint a running sandbox (snapshot + resume, VM keeps running)
    Checkpoint {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Custom checkpoint name (default: auto-generated)
        #[arg(long)]
        name: Option<String>,
        /// Compatibility flag; checkpoint always waits for snapshot creation
        #[arg(long)]
        blocking: bool,
    },
    /// Restore a suspended sandbox (shorthand for `start --checkpoint suspend`)
    Restore {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Block until guest reports ready (or timeout)
        #[arg(long)]
        blocking: bool,
        /// Ready timeout in seconds for --blocking
        #[arg(long)]
        ready_timeout_secs: Option<u32>,
        /// Exact S3 suspend snapshot URI to restore instead of local artifacts.
        #[arg(long)]
        suspend_restore_storage_uri: Option<String>,
    },
    /// Enable rolling btrfs snapshots for a sandbox
    EnableSnapshots {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Disable rolling btrfs snapshots for a sandbox
    DisableSnapshots {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// List btrfs snapshots for a sandbox (or delete with --delete/--delete-all)
    Snapshots {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Delete a specific snapshot by name
        #[arg(long = "delete")]
        delete: Option<String>,
        /// Delete all snapshots
        #[arg(long = "delete-all")]
        delete_all: bool,
    },
    /// Diagnose sandbox health: process status, CPU/MEM, zombie detection
    Doctor {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
    },
    /// Roll back a sandbox's filesystem to a previous snapshot
    Rollback {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Snapshot name to roll back to (e.g. "1m-20260331T140000Z")
        snapshot: String,
        /// Don't restart the sandbox after rollback
        #[arg(long)]
        no_restart: bool,
    },
    /// Take a manual snapshot of a sandbox
    Snapshot {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Take a full snapshot (pause + vmstate + full memory)
        #[arg(long, conflicts_with = "disk")]
        full: bool,
        /// Explicitly select the default disk-only snapshot (instant btrfs, no pause)
        #[arg(long)]
        disk: bool,
        /// Compatibility flag; snapshot always waits for snapshot creation
        #[arg(long)]
        blocking: bool,
    },
    /// Wait for a sandbox to reach a terminal state (stopped, failed, suspended)
    Wait {
        /// Sandbox ID, long UUID prefix, or short label
        sandbox_id: SandboxSelector,
        /// Timeout in seconds
        #[arg(long)]
        timeout: Option<u32>,
    },
}
