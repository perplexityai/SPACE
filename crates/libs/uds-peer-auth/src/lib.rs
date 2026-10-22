//! Shared UDS peer credential helpers for local SPACE service authorization.
//!
//! Unix socket permissions answer "who can connect". These helpers answer the
//! narrower service question "which local principal is this request from, and
//! is it allowed to perform this operation?".
//!
//! It is its own crate so a service can authorize UDS peers without
//! depending on the rest of `pplx-space-common-svc`.
//!
//! Authentication layers, weakest to strongest:
//!
//! 1. **Filesystem permissions** — the UDS itself is mode `0o770` with a
//!    SPACE-specific group, gating who can `connect(2)` at all.
//! 2. **`SO_PEERCRED`** — the kernel records `(pid, uid, gid)` on the
//!    server side at connect time; tokio surfaces this via `UCred` and
//!    we trust it to identify the calling user. This is what
//!    [`peer_identity_from_uds_connect_info`] reads.
//! 3. **Process inspection** — optional, opt-in per [`UdsPeerAccount`].
//!    After `SO_PEERCRED` identifies the caller's `(pid, uid)`, the
//!    authorizer reads `/proc/<pid>/exe` and `/proc/<pid>/status` to
//!    further verify the binary identity and Linux capabilities of the
//!    peer process. The effective uid from `/proc/<pid>/status` is
//!    cross-checked against the `SO_PEERCRED` uid to detect pid reuse
//!    between connect and the procfs read. This layer is governed by the
//!    [`PEER_PROCESS_CHECK_ENV`] switch ([`PeerProcessCheckMode`]): `off`
//!    (the default — disabled so local dev / non-Linux hosts aren't broken)
//!    skips it, `log-only` warns without rejecting, and `enforce` rejects on
//!    failure. A set-but-unrecognized value is a startup error, never a
//!    silent fallback to `off`. Layers 1–2 are unaffected in every mode.
//!
//! ## Kernel dependency: cross-uid `/proc` reads need `CAP_SYS_PTRACE`
//!
//! `/proc/<pid>/exe` readlink is gated by
//! `ptrace_may_access(PTRACE_MODE_READ_FSCREDS)` (see proc(5)). Its DAC
//! step admits only a reader whose uids match the target's, or one holding
//! `CAP_SYS_PTRACE` — so a cross-uid readlink **always** fails `EACCES`
//! for an unprivileged reader. Yama's `ptrace_scope` sysctl only further
//! restricts `PTRACE_MODE_ATTACH` (see ptrace(2)); no scope value,
//! including 0, relaxes the READ gate. A service with cross-uid
//! `require_binary` checks (e.g., credmgr running as `space-sbxmgr`
//! reading `space-tmplmgr` peers) therefore needs effective
//! `CAP_SYS_PTRACE` (systemd `AmbientCapabilities=CAP_SYS_PTRACE`, or the
//! pod securityContext's `capabilities.add`), or the peers must share its
//! uid. `/proc/<pid>/status`, where the uid cross-check and `CapEff` come
//! from, is world-readable and unaffected. The authorizer warns when an
//! armed mode is wired without that privilege
//! (see [`UdsPeerAuthorizer::with_process_check_mode`]), so the dependency
//! is visible in service logs rather than silently denying peers.
//!
//! `pidfd_open(2)` does **not** bypass this check — `/proc/<pid>/...`
//! reads go through `ptrace_may_access` regardless of how the pid was
//! identified. The race-mitigation benefit of pidfd is orthogonal to the
//! permission gate and is not pursued here.
//!
//! ## Where this layer is live
//!
//! The process checks run only where the opted-in service (credmgr)
//! authorizes UDS peers itself: the authority role serving its Unix socket,
//! i.e. direct deployments and local stacks. The TCP-only control-plane
//! authority authenticates ServiceAccount tokens instead. The node agent's
//! listener layer authorizes the distinct volume-manager UID and restricts its
//! separate socket to transfer credential RPCs before forwarding. The agent
//! also authorizes sandbox-manager for local CA issuance and injection-rule
//! resolution. These agent checks have no binary/capability requirements;
//! enabling the process-check switch there does not add `/proc` inspection.
//! A process-inspection bake must use an authority-with-UDS deployment that
//! configures per-account binary/capability requirements.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use http::Request as HttpRequest;
use tonic::transport::server::UdsConnectInfo;
use tonic::{Request as TonicRequest, Status};

use pplx_space_common_core::host::linux::paths::{
    SPACE_SYSTEMD_APPS_DIR, SPACE_SYSTEMD_BIN_DIR, is_content_addressed_binary_file_name,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PeerIdentity {
    uid: u32,
    gid: u32,
    pid: Option<i32>,
}

impl PeerIdentity {
    pub fn new(uid: u32, gid: u32, pid: Option<i32>) -> Self {
        Self { uid, gid, pid }
    }

    pub fn uid(&self) -> u32 {
        self.uid
    }

    pub fn gid(&self) -> u32 {
        self.gid
    }

    pub fn pid(&self) -> Option<i32> {
        self.pid
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalRole {
    Observer,
    Operator,
    Admin,
}

impl LocalRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observer => "observer",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }

    fn permits(self, allowed: Self) -> bool {
        matches!(
            (self, allowed),
            (Self::Observer, Self::Observer)
                | (Self::Operator, Self::Observer | Self::Operator)
                | (Self::Admin, Self::Observer | Self::Operator | Self::Admin)
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UdsPeerPrincipal {
    Root,
    Service(&'static str),
    Role(LocalRole),
    Other,
}

impl UdsPeerPrincipal {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::Service(name) => name,
            Self::Role(role) => role.as_str(),
            Self::Other => "other",
        }
    }

    /// True if a peer holding `self` satisfies a `required` principal drawn
    /// from an allow-list. `Role` principals honor the [`LocalRole`]
    /// hierarchy (`Admin` satisfies `Operator` satisfies `Observer`); every
    /// other principal must match exactly. This is the per-pair check behind
    /// allow-list membership, exposed so callers that resolve the principal
    /// themselves (e.g. multi-transport authorizers) can apply UDS policy
    /// without re-deriving the role semantics.
    pub fn satisfies(self, required: UdsPeerPrincipal) -> bool {
        match (self, required) {
            (Self::Role(peer), Self::Role(required)) => peer.permits(required),
            _ => self == required,
        }
    }
}

/// True if `principal` is an on-node operator permitted to set a node-local
/// (`authority=local`) administrator-owned policy: `root`, or a peer holding at least
/// the `Operator` role. Forwarding service principals (node-api, node-ctl) are
/// not, so they can only ever drive cluster-authority policy.
pub fn peer_may_set_local_authority(principal: UdsPeerPrincipal) -> bool {
    matches!(principal, UdsPeerPrincipal::Root)
        || principal.satisfies(UdsPeerPrincipal::Role(LocalRole::Operator))
}

/// Reject a request that sets a node-administrator-owned policy authority
/// (`authority=local`, for network or auto-lifecycle policy) from a peer that is
/// not an on-node operator. Single source of truth for this boundary, shared by
/// sandbox-manager and template-manager so their guards cannot diverge.
pub fn require_local_authority_peer(
    principal: UdsPeerPrincipal,
    is_local_authority: bool,
) -> Result<(), Status> {
    if is_local_authority && !peer_may_set_local_authority(principal) {
        return Err(Status::permission_denied(
            "authority=local requires a local operator peer; \
             forwarded callers may only set authority=cluster",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UdsPeerAccount {
    name: &'static str,
    fallback_uid: Option<u32>,
    principal: UdsPeerPrincipal,
    process_checks: PeerProcessChecks,
}

impl UdsPeerAccount {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            fallback_uid: None,
            principal: UdsPeerPrincipal::Service(name),
            process_checks: PeerProcessChecks::EMPTY,
        }
    }

    pub const fn with_fallback_uid(name: &'static str, fallback_uid: u32) -> Self {
        Self {
            name,
            fallback_uid: Some(fallback_uid),
            principal: UdsPeerPrincipal::Service(name),
            process_checks: PeerProcessChecks::EMPTY,
        }
    }

    pub const fn with_linux_user(
        user: pplx_space_common_core::host::linux::identities::LinuxUser,
    ) -> Self {
        Self {
            name: user.name,
            fallback_uid: Some(user.uid),
            principal: UdsPeerPrincipal::Service(user.name),
            process_checks: PeerProcessChecks::EMPTY,
        }
    }

    pub const fn role(name: &'static str, role: LocalRole) -> Self {
        Self {
            name,
            fallback_uid: None,
            principal: UdsPeerPrincipal::Role(role),
            process_checks: PeerProcessChecks::EMPTY,
        }
    }

    pub const fn role_with_fallback_uid(
        name: &'static str,
        role: LocalRole,
        fallback_uid: u32,
    ) -> Self {
        Self {
            name,
            fallback_uid: Some(fallback_uid),
            principal: UdsPeerPrincipal::Role(role),
            process_checks: PeerProcessChecks::EMPTY,
        }
    }

    /// Require the peer process's `/proc/<pid>/exe` to satisfy one of
    /// the listed stable install paths. This is verified after
    /// `SO_PEERCRED` matches the account, and after re-checking the
    /// peer's effective uid in `/proc/<pid>/status` to detect pid
    /// reuse. An entry admits the observed exe literally, as the
    /// entry's current symlink target (resolved at check time), or as
    /// the entry's content-addressed systemd install — the kernel
    /// resolves `/proc/<pid>/exe` fully, so a peer execed through an
    /// install symlink never reports the stable link path itself.
    ///
    /// Pass an empty slice to clear a previously-configured allowlist.
    pub const fn require_binary(mut self, allowed: &'static [&'static str]) -> Self {
        self.process_checks.binary_paths = allowed;
        self
    }

    /// Require the peer's effective capability set to contain none of
    /// the bits in `mask`. Use the constants in [`capability`] to
    /// compose masks. Useful as defense-in-depth (e.g., a service
    /// known to never need `CAP_SYS_PTRACE`).
    pub const fn forbid_capabilities(mut self, mask: u64) -> Self {
        self.process_checks.forbidden_caps = mask;
        self
    }
}

/// Optional process-level checks applied to the peer of a UDS
/// connection. Empty by default; opt in via the `require_*` /
/// `forbid_*` builder methods on [`UdsPeerAccount`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PeerProcessChecks {
    /// Allowlist of stable install paths for the peer's binary. The
    /// observed `/proc/<pid>/exe` matches an entry literally, as the
    /// entry's current symlink target (resolved at check time), or as
    /// the entry's content-addressed systemd install under
    /// `/space/apps`. Empty disables the check.
    binary_paths: &'static [&'static str],
    /// Bits that MUST be absent from the peer's effective capability
    /// set. `0` disables this half of the capability check.
    forbidden_caps: u64,
}

impl PeerProcessChecks {
    pub const EMPTY: Self = Self {
        binary_paths: &[],
        forbidden_caps: 0,
    };

    /// True iff at least one process-level check is enabled.
    pub const fn is_configured(&self) -> bool {
        !self.binary_paths.is_empty() || self.forbidden_caps != 0
    }
}

impl Default for PeerProcessChecks {
    fn default() -> Self {
        Self::EMPTY
    }
}

/// Bitmask helpers for selected Linux capabilities.
///
/// Values match `linux/capability.h` bit positions. Compose with `|`
/// to build masks for [`UdsPeerAccount::forbid_capabilities`]. Add more
/// constants here as needed; only the ones SPACE actually checks are
/// listed today.
pub mod capability {
    /// Override file ownership and ACL checks.
    pub const CAP_DAC_OVERRIDE: u64 = 1 << 1;
    /// Configure network interfaces, routing, etc.
    pub const CAP_NET_ADMIN: u64 = 1 << 12;
    /// Use raw / packet sockets.
    pub const CAP_NET_RAW: u64 = 1 << 13;
    /// Trace arbitrary processes (ptrace, /proc memory access).
    pub const CAP_SYS_PTRACE: u64 = 1 << 19;
    /// Range of system administration powers (mount, unshare, etc.).
    pub const CAP_SYS_ADMIN: u64 = 1 << 21;
}

/// Environment variable selecting how the `/proc`-based peer-process checks
/// (binary identity + capabilities) behave.
///
/// `SO_PEERCRED` uid matching is always enforced and is **not** affected by
/// this switch; only the layered process inspection is.
///
/// **Defaults to `off` when unset** (see [`PeerProcessCheckMode::from_env`]).
/// This is deliberate: the `/proc` reads depend on a Linux node with the
/// expected install paths and, for cross-uid peers, `CAP_SYS_PTRACE` — none
/// of which hold on a developer's laptop or in many local/CI setups. Defaulting off keeps the
/// mechanism from breaking local dev. A **set but unrecognized** value is a
/// startup error, not a fallback to `off` — a typo in a values file must not
/// silently disarm the control it was written to arm. Enable `log-only`
/// (observe without rejecting) or `enforce` only after verifying the install
/// surface for the target environment.
pub const PEER_PROCESS_CHECK_ENV: &str = "SPACE_UDS_PEER_PROCESS_CHECK";

/// How the opt-in `/proc`-based peer-process checks behave.
///
/// Wired explicitly via [`UdsPeerAuthorizer::with_process_check_mode`];
/// [`PeerProcessCheckMode::from_env`] is the standard source for services
/// that opt in. Defaults to [`Off`](Self::Off) so the mechanism never breaks
/// a host that can't satisfy the `/proc` preconditions (notably local dev);
/// environments that can run it opt in explicitly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PeerProcessCheckMode {
    /// Skip the process checks entirely. `SO_PEERCRED` uid matching still
    /// applies. The default, and the dev-safe state.
    #[default]
    Off,
    /// Run the checks and log failures, but allow the RPC through. Use during
    /// rollout to observe what enforcement *would* reject.
    LogOnly,
    /// Run the checks and reject the RPC on any failure.
    Enforce,
}

impl PeerProcessCheckMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::LogOnly => "log-only",
            Self::Enforce => "enforce",
        }
    }

    /// True when a failed check should reject the RPC (as opposed to log-only).
    const fn enforces(self) -> bool {
        matches!(self, Self::Enforce)
    }

    /// Reads the mode from [`PEER_PROCESS_CHECK_ENV`]. Absent means
    /// [`Off`](Self::Off), the dev-safe default. A present-but-unrecognized
    /// or non-unicode value is an **error**: the switch arms a security
    /// control, and a typo (`enfroce`) resolving to `off` would disarm it
    /// with nothing but a log line. Callers fail startup on `Err`.
    pub fn from_env() -> Result<Self, PeerProcessCheckModeEnvError> {
        Self::from_env_value(std::env::var(PEER_PROCESS_CHECK_ENV))
    }

    /// The [`from_env`](Self::from_env) mapping over an injected read, so
    /// tests cover it without mutating process-global state.
    fn from_env_value(
        raw: Result<String, std::env::VarError>,
    ) -> Result<Self, PeerProcessCheckModeEnvError> {
        match raw {
            Ok(raw) => Ok(raw.parse()?),
            Err(std::env::VarError::NotPresent) => Ok(Self::Off),
            Err(std::env::VarError::NotUnicode(_)) => Err(PeerProcessCheckModeEnvError::NotUnicode),
        }
    }
}

/// Failures reading [`PEER_PROCESS_CHECK_ENV`]. Both variants mean the
/// variable is set to something no operator can have meant; treat them as
/// startup failures rather than running with a mode the operator didn't pick.
#[derive(Debug, thiserror::Error)]
pub enum PeerProcessCheckModeEnvError {
    #[error("{env}: {0}", env = PEER_PROCESS_CHECK_ENV)]
    Invalid(#[from] ParsePeerProcessCheckModeError),
    #[error("{env} holds a non-unicode value", env = PEER_PROCESS_CHECK_ENV)]
    NotUnicode,
}

impl std::fmt::Display for PeerProcessCheckMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid UDS peer-process check mode `{0}` (expected `enforce`, `log-only`, or `off`)")]
pub struct ParsePeerProcessCheckModeError(String);

impl std::str::FromStr for PeerProcessCheckMode {
    type Err = ParsePeerProcessCheckModeError;

    // Exactly the three canonical spellings (whitespace- and
    // case-insensitive). No synonyms: a security switch that accepts loose
    // spellings invites false confidence — "kill" reading as `off` would
    // disable enforcement while sounding like it rejects RPCs.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "enforce" => Ok(Self::Enforce),
            "log-only" => Ok(Self::LogOnly),
            "off" => Ok(Self::Off),
            _ => Err(ParsePeerProcessCheckModeError(s.to_owned())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct AuthorizedUdsPeer {
    identity: PeerIdentity,
    principal: UdsPeerPrincipal,
}

impl AuthorizedUdsPeer {
    pub fn identity(&self) -> &PeerIdentity {
        &self.identity
    }

    pub fn principal(&self) -> UdsPeerPrincipal {
        self.principal
    }
}

#[derive(Clone, Debug)]
pub struct UdsPeerAuthorizer {
    service_name: Arc<str>,
    accounts: Vec<ResolvedUdsPeerAccount>,
    process_check_mode: PeerProcessCheckMode,
}

impl UdsPeerAuthorizer {
    /// Construction never consults the environment: the mode defaults to
    /// [`PeerProcessCheckMode::Off`], and services that opt into the process
    /// checks resolve a mode explicitly (normally
    /// [`PeerProcessCheckMode::from_env`], failing startup on an invalid
    /// value) and wire it via
    /// [`with_process_check_mode`](Self::with_process_check_mode).
    pub fn new(
        service_name: impl Into<Arc<str>>,
        accounts: impl IntoIterator<Item = UdsPeerAccount>,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            accounts: accounts.into_iter().map(resolve_account).collect(),
            process_check_mode: PeerProcessCheckMode::Off,
        }
    }

    /// Sets the [`PeerProcessCheckMode`].
    pub fn with_process_check_mode(mut self, mode: PeerProcessCheckMode) -> Self {
        self.process_check_mode = mode;
        // The cross-uid readability preflight concerns only an armed layer;
        // warning under `Off` would alarm about a control that isn't running.
        if mode != PeerProcessCheckMode::Off {
            self.warn_if_cross_uid_proc_unreadable();
        }
        self
    }

    pub fn new_with_default_local_roles(
        service_name: impl Into<Arc<str>>,
        accounts: impl IntoIterator<Item = UdsPeerAccount>,
    ) -> Self {
        Self::new(
            service_name,
            accounts.into_iter().chain(default_local_role_accounts()),
        )
    }

    /// Internal classification that also returns the matched account
    /// so process-level checks can reach the per-account
    /// [`PeerProcessChecks`].
    fn classify<'a>(&'a self, identity: &PeerIdentity) -> ClassifiedPeer<'a> {
        if identity.uid == 0 {
            return ClassifiedPeer {
                principal: UdsPeerPrincipal::Root,
                account: None,
            };
        }

        for account in &self.accounts {
            if account.uids.contains(&identity.uid) {
                return ClassifiedPeer {
                    principal: account.principal,
                    account: Some(account),
                };
            }
        }

        ClassifiedPeer {
            principal: UdsPeerPrincipal::Other,
            account: None,
        }
    }

    pub fn authorize_request<T>(
        &self,
        request: &TonicRequest<T>,
        operation: &str,
        allowed: &[UdsPeerPrincipal],
    ) -> Result<AuthorizedUdsPeer, Status> {
        let identity = peer_identity_from_tonic_request(request).ok_or_else(|| {
            tracing::warn!(
                service = %self.service_name,
                operation,
                "rejecting local RPC without UDS peer credentials"
            );
            Status::unauthenticated("UDS peer credentials missing")
        })?;
        self.authorize_identity(identity, operation, allowed)
    }

    pub fn authorize_identity(
        &self,
        identity: PeerIdentity,
        operation: &str,
        allowed: &[UdsPeerPrincipal],
    ) -> Result<AuthorizedUdsPeer, Status> {
        let classified = self.classify(&identity);
        let principal = classified.principal;

        if !principal_allowed(principal, allowed) {
            tracing::warn!(
                service = %self.service_name,
                operation,
                peer_uid = identity.uid(),
                peer_gid = identity.gid(),
                peer_pid = ?identity.pid(),
                peer_principal = principal.as_str(),
                allowed_principals = ?allowed.iter().map(|principal| principal.as_str()).collect::<Vec<_>>(),
                "rejecting unauthorized local RPC"
            );
            return Err(Status::permission_denied(format!(
                "local peer {} is not authorized for {operation}",
                principal.as_str()
            )));
        }

        // Record on the gRPC server span (declared in
        // `common-svc::bootstrap::GrpcServerMakeSpan`) so every handler that
        // delegates UDS authorization here surfaces `rpc.client.peer_principal`
        // without each service having to record it locally.
        tracing::Span::current().record("rpc.client.peer_principal", principal.as_str());

        self.verify_classified_process_checks(&identity, &classified, operation)?;

        Ok(AuthorizedUdsPeer {
            identity,
            principal,
        })
    }

    /// Process-level checks fire only for matched non-root accounts that
    /// opted in. Root and `Other` peers go through the same classification
    /// but never have an account attached. The `Off` kill switch skips them
    /// entirely; `SO_PEERCRED` matching is unaffected in every mode.
    fn verify_classified_process_checks(
        &self,
        identity: &PeerIdentity,
        classified: &ClassifiedPeer<'_>,
        operation: &str,
    ) -> Result<(), Status> {
        if self.process_check_mode == PeerProcessCheckMode::Off {
            return Ok(());
        }
        let Some(account) = classified.account else {
            return Ok(());
        };
        if !account.process_checks.is_configured() {
            return Ok(());
        }
        self.run_process_checks(identity, account, operation)
    }

    /// Pre-flight diagnostic: emit a single warning when an armed mode is
    /// wired, some configured account's checks apply to peers of a
    /// different uid, and this process lacks the privilege to read a
    /// cross-uid peer's `/proc/<pid>/exe` (effective `CAP_SYS_PTRACE`, or
    /// euid 0 which implies it). In that configuration every such peer
    /// fails `proc_unreadable` — under enforce, denied — regardless of
    /// yama's `ptrace_scope`, which gates only ATTACH. Surfaced once, when
    /// the mode is wired, so it's visible in service logs before any
    /// failing request. No-op on non-Linux (the procfs path doesn't exist;
    /// opt-in checks already fail closed there).
    fn warn_if_cross_uid_proc_unreadable(&self) {
        #[cfg(target_os = "linux")]
        {
            let our_euid = nix::unistd::geteuid().as_raw();
            // An unreadable /proc/self/status reads as "no capabilities",
            // failing toward the warning rather than suppressing it.
            let our_caps = os::current_process_cap_effective().unwrap_or(0);
            if cross_uid_checks_unreadable(&self.accounts, our_euid, our_caps) {
                tracing::warn!(
                    service = %self.service_name,
                    our_euid,
                    "peer process checks are configured for peers of a different uid, but this \
                     process cannot read a cross-uid /proc/<pid>/exe: the DAC half of \
                     ptrace_may_access requires matching uids or CAP_SYS_PTRACE, and yama's \
                     ptrace_scope never relaxes it. Grant this service effective CAP_SYS_PTRACE \
                     (systemd AmbientCapabilities / pod securityContext capabilities.add) or run \
                     those peers under its uid; otherwise they fail proc_unreadable."
                );
            }
        }
    }

    /// Read `/proc/<pid>/{exe,status}` for the peer and apply the
    /// account's [`PeerProcessChecks`]. In [`PeerProcessCheckMode::Enforce`]
    /// failures are mapped to `PERMISSION_DENIED` (or `UNAUTHENTICATED` when
    /// peer info is unavailable) and the RPC is rejected; in
    /// [`PeerProcessCheckMode::LogOnly`] the same conditions are logged but the
    /// RPC is allowed through. Always fails closed in enforce mode (a peer we
    /// cannot inspect is rejected).
    fn run_process_checks(
        &self,
        identity: &PeerIdentity,
        account: &ResolvedUdsPeerAccount,
        operation: &str,
    ) -> Result<(), Status> {
        let principal = account.principal.as_str();

        let Some(pid) = identity.pid else {
            return self.process_check_failure_verdict(
                identity,
                principal,
                operation,
                "pid_unavailable",
                "peer pid unavailable",
                Status::unauthenticated("peer pid unavailable; cannot verify process identity"),
            );
        };

        // Blocking reads from async handlers, deliberately: procfs and the
        // dcache answer in microseconds and only armed accounts reach this
        // path, so spawn_blocking would cost more than it saves.
        let snapshot = match read_peer_process(pid) {
            Ok(snapshot) => snapshot,
            Err(err) => {
                return self.process_check_failure_verdict(
                    identity,
                    principal,
                    operation,
                    "proc_unreadable",
                    &err.to_string(),
                    Status::unauthenticated(format!("cannot inspect peer process: {err}")),
                );
            }
        };

        // A deleted backing binary is its own signal: the on-disk executable
        // was unlinked out from under a running, otherwise-trusted process
        // (typically an in-place upgrade, but also a tamper indicator). The
        // allowlist comparison runs against the `(deleted)`-stripped path so
        // a mid-upgrade peer is still recognized. Counted rather than warned:
        // a replaced-but-not-restarted peer would otherwise warn on every RPC
        // indefinitely, not just during the swap window.
        if snapshot.exe_deleted {
            tracing::debug!(
                service = %self.service_name,
                operation,
                peer_uid = identity.uid(),
                peer_pid = pid,
                peer_principal = principal,
                peer_exe = %snapshot.exe.display(),
                "peer process backing binary is deleted (in-place upgrade or tampering)"
            );
            pplx_space_metrics::auth::uds_peer_exe_deleted(&self.service_name, principal);
        }

        if let Err(err) = evaluate_process_checks(
            &account.process_checks,
            identity.uid(),
            &snapshot,
            resolve_binary_allowlist_targets,
        ) {
            return self.process_check_failure_verdict(
                identity,
                principal,
                operation,
                err.metric_reason(),
                &err.to_string(),
                Status::permission_denied(format!("peer process check failed: {err}")),
            );
        }

        pplx_space_metrics::auth::uds_peer_process_check(
            &self.service_name,
            "allow",
            "ok",
            principal,
            self.process_check_mode.as_str(),
        );
        Ok(())
    }

    /// Terminal handling for one failed process-check step: log it, emit the
    /// outcome counter (every evaluation exits through exactly one emission,
    /// so a log-only bake reads `result:allow, reason!=ok` and enforce
    /// denials are alertable), and apply the enforce/log-only split — deny
    /// with `status_if_enforced` when enforcing, allow otherwise.
    fn process_check_failure_verdict(
        &self,
        identity: &PeerIdentity,
        principal: &'static str,
        operation: &str,
        reason: &'static str,
        detail: &str,
        status_if_enforced: Status,
    ) -> Result<(), Status> {
        let enforced = self.process_check_mode.enforces();
        tracing::warn!(
            service = %self.service_name,
            operation,
            peer_uid = identity.uid(),
            peer_pid = ?identity.pid(),
            peer_principal = principal,
            mode = self.process_check_mode.as_str(),
            enforced,
            reason,
            detail,
            "peer process check step failed"
        );
        pplx_space_metrics::auth::uds_peer_process_check(
            &self.service_name,
            if enforced { "deny" } else { "allow" },
            reason,
            principal,
            self.process_check_mode.as_str(),
        );
        if enforced {
            Err(status_if_enforced)
        } else {
            Ok(())
        }
    }
}

struct ClassifiedPeer<'a> {
    principal: UdsPeerPrincipal,
    account: Option<&'a ResolvedUdsPeerAccount>,
}

#[derive(Clone, Debug)]
struct ResolvedUdsPeerAccount {
    principal: UdsPeerPrincipal,
    uids: Vec<u32>,
    process_checks: PeerProcessChecks,
}

/// Pure decision behind the cross-uid preflight warning: true when some
/// account's configured checks apply to peers whose uid differs from ours
/// AND we lack the privilege to read a cross-uid `/proc/<pid>/exe`
/// (effective `CAP_SYS_PTRACE`, or euid 0). Deliberately independent of
/// yama's `ptrace_scope`: the READ-mode DAC gate is what bites, and no
/// scope value relaxes it.
#[cfg(any(target_os = "linux", test))]
fn cross_uid_checks_unreadable(
    accounts: &[ResolvedUdsPeerAccount],
    our_euid: u32,
    our_cap_effective: u64,
) -> bool {
    let privileged = our_euid == 0 || our_cap_effective & capability::CAP_SYS_PTRACE != 0;
    if privileged {
        return false;
    }
    accounts.iter().any(|account| {
        account.process_checks.is_configured() && account.uids.iter().any(|uid| *uid != our_euid)
    })
}

fn resolve_account(account: UdsPeerAccount) -> ResolvedUdsPeerAccount {
    let mut uids = Vec::new();
    if let Some(uid) = account.fallback_uid {
        uids.push(uid);
    }
    if let Some(uid) = uid_for_account(account.name) {
        uids.push(uid);
    }
    uids.sort_unstable();
    uids.dedup();
    ResolvedUdsPeerAccount {
        principal: account.principal,
        uids,
        process_checks: account.process_checks,
    }
}

fn default_local_role_accounts() -> impl IntoIterator<Item = UdsPeerAccount> {
    [
        UdsPeerAccount::role("dev", LocalRole::Observer),
        UdsPeerAccount::role("deploy", LocalRole::Operator),
    ]
}

fn principal_allowed(principal: UdsPeerPrincipal, allowed: &[UdsPeerPrincipal]) -> bool {
    allowed.iter().any(|allowed| principal.satisfies(*allowed))
}

fn uid_for_account(account_name: &str) -> Option<u32> {
    nix::unistd::User::from_name(account_name)
        .ok()
        .flatten()
        .map(|user| user.uid.as_raw())
}

pub fn peer_identity_from_tonic_request<T>(request: &TonicRequest<T>) -> Option<PeerIdentity> {
    if let Some(identity) = request.extensions().get::<PeerIdentity>() {
        return Some(identity.clone());
    }

    request
        .extensions()
        .get::<UdsConnectInfo>()
        .and_then(peer_identity_from_uds_connect_info)
}

pub fn peer_identity_from_http_request<T>(request: &HttpRequest<T>) -> Option<PeerIdentity> {
    if let Some(identity) = request.extensions().get::<PeerIdentity>() {
        return Some(identity.clone());
    }

    request
        .extensions()
        .get::<UdsConnectInfo>()
        .and_then(peer_identity_from_uds_connect_info)
}

fn peer_identity_from_uds_connect_info(info: &UdsConnectInfo) -> Option<PeerIdentity> {
    let cred = info.peer_cred?;
    Some(PeerIdentity::new(cred.uid(), cred.gid(), cred.pid()))
}

mod os;

use os::read_peer_process;

/// Snapshot of a peer process derived from `/proc/<pid>/{exe,status}`.
/// Captured eagerly so the rest of the check is a pure function over
/// these values.
#[derive(Clone, Debug, Eq, PartialEq)]
struct PeerProcessSnapshot {
    /// Resolved target of `/proc/<pid>/exe`, with any kernel-appended
    /// ` (deleted)` suffix stripped by the Linux reader's `normalize_exe`.
    exe: PathBuf,
    /// Whether the raw `/proc/<pid>/exe` readlink carried the kernel's
    /// ` (deleted)` suffix before normalization. Surfaced so the authorizer
    /// can log the deleted-binary signal; it does not by itself fail any check.
    exe_deleted: bool,
    /// Effective uid from the `Uid:` line of `/proc/<pid>/status`.
    /// Compared against the `SO_PEERCRED` uid to detect pid reuse
    /// between connect and the procfs read.
    effective_uid: u32,
    /// `CapEff:` line from `/proc/<pid>/status`, parsed from hex.
    cap_effective: u64,
}

/// Failures from [`evaluate_process_checks`]. The variants shape the
/// authorizer's tracing fields and become the body of the
/// `PERMISSION_DENIED` Status returned to the peer.
#[derive(Debug, thiserror::Error)]
enum ProcessCheckFailure {
    #[error(
        "peer effective uid changed after connect (SO_PEERCRED uid={expected}, /proc effective uid={found}); pid likely reused"
    )]
    UidDrift { expected: u32, found: u32 },
    #[error("peer binary {0} is not in the configured allowlist")]
    BinaryNotAllowed(String),
    #[error("peer holds forbidden capabilities (mask 0x{0:016x})")]
    HoldsForbiddenCapabilities(u64),
}

impl ProcessCheckFailure {
    /// Bounded tag value for the outcome counter.
    fn metric_reason(&self) -> &'static str {
        match self {
            Self::UidDrift { .. } => "uid_drift",
            Self::BinaryNotAllowed(_) => "binary_not_allowed",
            Self::HoldsForbiddenCapabilities(_) => "forbidden_capabilities",
        }
    }
}

/// Resolve each configured allowlist entry through the filesystem *at
/// check time*, yielding the canonical file its symlink chain currently
/// points at. Entries that don't exist on this host (the other layout's
/// path) resolve to nothing. Check-time rather than startup so a deploy
/// repointing a `/space/bin` symlink is seen by the next RPC instead of
/// going stale with the process.
fn resolve_binary_allowlist_targets(paths: &[&str]) -> Vec<PathBuf> {
    paths
        .iter()
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .collect()
}

/// Which filesystem-free form of `entry` the observed `/proc/<pid>/exe`
/// satisfies: the entry itself (`literal`) or the content-addressed install
/// the systemd deploy script produces for it (`content_addressed`). The
/// third admitted form — the entry's current symlink target — needs a
/// filesystem read and is supplied by the caller via
/// [`resolve_binary_allowlist_targets`].
fn exe_allowlist_match_form(entry: &Path, exe: &Path) -> Option<&'static str> {
    if exe == entry {
        Some("literal")
    } else if is_content_addressed_install(exe, entry) {
        Some("content_addressed")
    } else {
        None
    }
}

/// The supported systemd deployment layout installs each service build at
/// `/space/apps/<name>-<sha256[:8]>` and repoints the stable
/// `/space/bin/<name>` symlink; execs through the symlink make
/// `/proc/<pid>/exe` report the digest target. Admitting the shape keeps
/// a peer still running the pre-swap build authorized during the window
/// between `ln -sfn` and its service restart, which check-time symlink
/// resolution alone would deny. Writing under `/space/apps` requires the
/// same privilege as writing `/space/bin`, so this admits no one the
/// literal check would have kept out.
///
/// Applies only to entries under the systemd bin dir: that layout is the
/// sole producer of the digest shape, and letting it satisfy a Helm-layout
/// entry (a real file under `/opt/space/bin`) would widen that entry for
/// no reachable install.
fn is_content_addressed_install(exe: &Path, entry: &Path) -> bool {
    if entry.parent() != Some(Path::new(SPACE_SYSTEMD_BIN_DIR)) {
        return false;
    }
    if exe.parent() != Some(Path::new(SPACE_SYSTEMD_APPS_DIR)) {
        return false;
    }
    let (Some(exe_name), Some(entry_name)) = (
        exe.file_name().and_then(|name| name.to_str()),
        entry.file_name().and_then(|name| name.to_str()),
    ) else {
        return false;
    };
    is_content_addressed_binary_file_name(exe_name, entry_name)
}

/// Decide whether a peer satisfies the configured process-level checks.
/// Pure function over a captured [`PeerProcessSnapshot`]; filesystem access
/// is confined to [`read_peer_process`] and the injected
/// `resolve_binary_targets` (normally
/// [`resolve_binary_allowlist_targets`]), which runs only when the string
/// forms of the binary check both miss.
fn evaluate_process_checks(
    checks: &PeerProcessChecks,
    expected_uid: u32,
    snapshot: &PeerProcessSnapshot,
    resolve_binary_targets: impl FnOnce(&'static [&'static str]) -> Vec<PathBuf>,
) -> Result<(), ProcessCheckFailure> {
    // Race mitigation: SO_PEERCRED captured uid at connect time. If
    // the pid was reused between connect and our /proc read, the
    // effective uid in /proc almost certainly differs. Verify match
    // before trusting the snapshot. Comparing the *effective* uid also
    // constrains a peer that seteuid()s mid-connection: peers are expected
    // to hold a stable euid for the connection's lifetime.
    if snapshot.effective_uid != expected_uid {
        return Err(ProcessCheckFailure::UidDrift {
            expected: expected_uid,
            found: snapshot.effective_uid,
        });
    }

    if !checks.binary_paths.is_empty() {
        // Symlink resolution costs filesystem reads per entry, so it runs
        // only when the literal and content-addressed forms both miss.
        let form = checks
            .binary_paths
            .iter()
            .find_map(|entry| exe_allowlist_match_form(Path::new(entry), &snapshot.exe))
            .or_else(|| {
                resolve_binary_targets(checks.binary_paths)
                    .contains(&snapshot.exe)
                    .then_some("resolved_symlink_target")
            });
        match form {
            // Which form matched is bake-time signal: a fleet matching only
            // via symlink resolution is one mount-namespace mismatch away
            // from denials.
            Some(form) => tracing::debug!(
                peer_exe = %snapshot.exe.display(),
                form,
                "peer binary allowlist matched"
            ),
            None => {
                return Err(ProcessCheckFailure::BinaryNotAllowed(
                    snapshot.exe.to_string_lossy().into_owned(),
                ));
            }
        }
    }

    let held = checks.forbidden_caps & snapshot.cap_effective;
    if held != 0 {
        return Err(ProcessCheckFailure::HoldsForbiddenCapabilities(held));
    }

    Ok(())
}

#[cfg(test)]
mod tests;
