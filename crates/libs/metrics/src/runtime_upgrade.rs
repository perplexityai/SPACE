//! Runtime upgrades decided when a sandbox resumes or starts.
//!
//! * `runtime_upgrade.activation.decision` (counter): one per resume or start
//!   the decision ran for. Tags: `activation` (`start` or `restore`), `plan`
//!   (`as_recorded`, `restore_on_default`, `cold_boot_upgrade`, `stranded`, or
//!   `unattributed` when no recorded execution names an environment the
//!   catalog knows), and `cause` (`blocked`, `unlisted`, `deprecated`, or
//!   `none` when the environment still executes).

use crate::emit::{counter, tag};

/// Count one resume- or start-time runtime upgrade decision.
pub fn activation_decision(activation: &'static str, plan: &'static str, cause: &'static str) {
    counter(
        "runtime_upgrade.activation.decision",
        1,
        &[
            tag("activation", activation),
            tag("plan", plan),
            tag("cause", cause),
        ],
    );
}
