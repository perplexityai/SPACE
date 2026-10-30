//! The abandoned-restore retirement switch the restore-revert writers share.

/// Whether a restore rollback retires the abandoned restore's execution as
/// `replaced`, and so makes its pod disposable and its captures refusable:
/// the API's rollback of a failed restore claim, the transient-lifecycle
/// sweeper's revert of an aged `restoring` row, and the internal API's
/// application of a node's activation-rollback receipt for a restore.
///
/// Retiring is safe only once no user request can have reached a `restoring`
/// sandbox's pod, which every routing replica (API and internal API) running
/// the restoring gate guarantees. A replica from before that gate still
/// routes to a reachable restore pod, and a retirement would then delete
/// what it served, or refuse the capture that holds it. So retirement ships
/// disabled, behind [`Self::ENV`], and is enabled per deployment once every
/// routing replica runs the gate; rolling back to older routing binaries
/// disables it first. Disabled, the API and the sweeper revert only the
/// lifecycle, and the internal API retires a rolled-back restore's execution
/// as `suspended`, as they did before.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AbandonedRestoreRetirement {
    /// The API and the sweeper revert only the lifecycle, leaving the
    /// restore's execution active; the internal API retires a rolled-back
    /// restore's execution as `suspended`.
    #[default]
    Disabled,
    /// Retire the abandoned restore's execution in the revert.
    Enabled,
}

impl AbandonedRestoreRetirement {
    /// The switch the API, the cluster controller and the internal API read
    /// ([`Self::from_env`]): `1`, `true`, `yes`, `on` or `enabled`, in any
    /// case, turns retirement on; unset or anything else keeps it disabled.
    pub const ENV: &'static str = "SPACE_ABANDONED_RESTORE_RETIREMENT_ENABLED";

    pub fn from_enabled(enabled: bool) -> Self {
        if enabled {
            Self::Enabled
        } else {
            Self::Disabled
        }
    }

    /// The switch as every component reads it, so they agree on what turns
    /// it on.
    pub fn from_env() -> Self {
        Self::from_enabled(crate::env::env_flag_var(Self::ENV, false))
    }

    pub fn is_enabled(self) -> bool {
        self == Self::Enabled
    }
}
