//! Unit coverage for the fence's classification and kill switch.
//!
//! The interleaving itself cannot be decided here — it needs two live
//! transactions and a real row lock, and lives in
//! `cluster-controller`'s `node_drain/store/placement_fence_tests.rs` next to
//! the phase writer it has to be ordered against.

use super::*;
use pplx_space_common_core::env::env_flag;

/// **`draining` is open, and that is the finding this test exists for.**
///
/// An intermediate revision delegated the phase policy to
/// `NodeDrainState::blocks_placement`, which refuses `draining` under a forced
/// drain. That inverted the outcome for the one phase where the fence had
/// something to lose: node-drain lists only `draining` nodes and its evacuation
/// queue selects `WHERE active_node_id = $1`, so refusing the placement write
/// there deletes the sandbox from the queue that would have migrated it, and the
/// node then drains and terminates with a guest nothing is looking for.
///
/// The `drain_mode` column is not read at all any more, which is why this asserts
/// on the phase alone: once `draining` is open regardless of mode, there is
/// nothing for the mode to decide and it stays out of the statement.
#[test]
fn a_draining_node_accepts_placement_in_either_mode() {
    assert_eq!(
        classify("draining"),
        PlacementFence::Open,
        "`draining` must stay open — refusing it removes the sandbox from the \
         evacuation queue that would have saved it"
    );
}

/// The two phases where no evacuation runs, so refusing costs nothing the
/// sandbox had. Paired with the test above, this is the whole policy.
#[test]
fn the_post_drain_phases_refuse() {
    assert_eq!(classify("drained"), PlacementFence::Drained);
    assert_eq!(classify("terminating"), PlacementFence::Terminating);
}

/// The kill switch's *default* and its *vocabulary* are two claims, and only the
/// first belongs here.
///
/// The vocabulary — which strings are falsy, that matching is case-insensitive
/// and trimmed, that an unrecognized value falls back to the caller's default —
/// is `pplx_space_common_core::env::env_flag`'s, and is tested in common-core.
/// Re-testing it here would be a second copy of a matrix that exists to have one
/// copy, and the copy would pass while the two implementations drifted, which is
/// how `disabled` came to disable every other switch in the fleet and not this
/// one.
///
/// What is this module's own claim is the *default*: a kill switch for a
/// data-loss guard must enforce when unset and when misspelled.
#[test]
fn the_fence_enforces_unless_explicitly_disabled() {
    for enabling in [None, Some(""), Some("true"), Some("1"), Some("flase")] {
        assert!(
            env_flag(enabling, true),
            "{enabling:?} must leave the fence enforcing"
        );
    }
    for disabling in [Some("false"), Some("0"), Some("off"), Some("disabled")] {
        assert!(
            !env_flag(disabling, true),
            "{disabling:?} must disable the fence"
        );
    }
}

/// Every refusing verdict refuses and `Open` does not — including the
/// fail-closed one, which is the variant a `matches!` written the other way
/// round would let through.
#[test]
fn every_refusing_verdict_refuses_and_open_does_not() {
    assert!(!PlacementFence::Open.refuses());
    for refusing in [
        PlacementFence::MissingNode,
        PlacementFence::Drained,
        PlacementFence::Terminating,
        PlacementFence::UnrecognizedPhase,
    ] {
        assert!(refusing.refuses(), "{refusing:?} must refuse");
        assert_ne!(
            refusing.as_str(),
            PlacementFence::Open.as_str(),
            "a refusal must not report the open tag"
        );
    }
}

/// The two modelled phases map to their own names, and the unparseable case does
/// not borrow one. Pinned because these strings are the `verdict` metric tag, so
/// renaming one silently breaks a monitor rather than a build.
#[test]
fn each_verdict_reports_its_own_tag() {
    assert_eq!(PlacementFence::MissingNode.as_str(), "missing_node");
    assert_eq!(classify("drained").as_str(), "drained");
    assert_eq!(classify("terminating").as_str(), "terminating");
    assert_eq!(classify("cordoning").as_str(), "unrecognized_phase");
    assert_eq!(classify("draining").as_str(), "open");
}
