//! The restore dispatch fence switch and its timing.
//!
//! A Kubernetes user restore whose owner dies before it sends its Wake would
//! otherwise block every re-drive for the restore lease and execution-age
//! windows ([`super::RESTORE_EXECUTION_OWNERSHIP_TTL_SECS`]), five minutes,
//! although nothing can be running for it on a node. With the fence on, the
//! owner proves on the execution that it hasn't sent the Wake, renewing that
//! proof while it is alive, and clears it under the sandbox row lock before
//! sending; a re-drive may take over once the proof has expired (see
//! `pplx_space_sandbox_store::restore_dispatch_fence`).

use std::time::Duration;

/// How long an owner's fence keeps a re-drive out without a renewal. It
/// bounds how long a dead owner's restore stays blocked, and has to outlast a
/// few missed renewals of a live one.
pub(super) const FENCE_TTL: Duration = Duration::from_secs(30);

/// How often a live owner renews its fence while it has not sent its Wake.
pub(super) const FENCE_RENEW_INTERVAL: Duration = Duration::from_secs(10);

const _: () = assert!(
    FENCE_TTL.as_secs() >= 3 * FENCE_RENEW_INTERVAL.as_secs(),
    "a live owner must survive two missed renewals",
);

/// Whether user restores write the dispatch fence and re-drives take over an
/// expired one (`SPACE_RESTORE_DISPATCH_FENCE_ENABLED`, default off).
///
/// Each half is safe alone, so replicas may disagree while the switch rolls
/// out: an owner with it off writes no fence, which keeps the restore lease
/// and execution-age guards for every re-drive; a re-drive with it off ignores
/// fences; and an owner with it on clears its fence, under its token, before
/// any Wake, whichever replica re-drives. Turning it off stops new fences and
/// takeovers; a fence left behind expires unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RestoreDispatchFence {
    #[default]
    Disabled,
    Enabled,
}

impl RestoreDispatchFence {
    /// The switch the API and the cluster controller read
    /// ([`Self::from_env`]): `1`, `true`, `yes`, `on` or `enabled`, in any
    /// case, turns it on; unset or anything else keeps it off.
    pub const ENV: &'static str = "SPACE_RESTORE_DISPATCH_FENCE_ENABLED";

    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    pub fn from_env() -> Self {
        Self::from_enabled(pplx_space_env_vars::env_flag_var(Self::ENV, false))
    }

    pub fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}
