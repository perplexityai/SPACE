use std::collections::BTreeMap;
use std::time::Duration;

use super::*;

const WINDOW: Duration = Duration::from_secs(600);
const INSTANCE: &str = "i-0123456789abcdef0";
const REFERENCE: &str = "i-0fedcba987654321f";

/// What EC2 lists for one ID in a sweep row: nothing, or a state.
#[derive(Debug, Clone, Copy)]
enum Listed {
    Absent,
    State(&'static str),
}

fn answered(reference: Listed, instance: Listed) -> InstanceEvidence {
    let mut listed = BTreeMap::new();
    for (id, entry) in [(REFERENCE, reference), (INSTANCE, instance)] {
        if let Listed::State(state) = entry {
            listed.insert(id.to_owned(), state.to_owned());
        }
    }
    InstanceEvidence::Answered {
        listed,
        reference: REFERENCE.to_owned(),
    }
}

/// Every reference answer × every instance answer × the call answering or
/// failing. Dead needs the call to answer, the reference listed in a live
/// state, and the instance absent or `terminated`; a state EC2 adds later
/// reads live, like `stopped`.
#[test]
fn instance_verdict_sweeps_reference_instance_and_call() {
    use InstanceVerdict as V;
    use UnknownReason as U;
    let references = [
        (Listed::State("running"), None),
        (Listed::State("stopped"), None),
        (Listed::Absent, Some(U::ReferenceMissing)),
        (Listed::State("terminated"), Some(U::ReferenceTerminated)),
    ];
    let instances = [
        (Listed::Absent, V::Dead(DeadReason::Absent)),
        (Listed::State("terminated"), V::Dead(DeadReason::Terminated)),
        (Listed::State("running"), V::Live),
        (Listed::State("stopped"), V::Live),
        (Listed::State("shutting-down"), V::Live),
        (Listed::State("hibernated-in-a-future-api"), V::Live),
    ];
    for (reference, reference_refusal) in references {
        for (instance, when_trusted) in instances {
            for call_ok in [true, false] {
                let evidence = if call_ok {
                    answered(reference, instance)
                } else {
                    InstanceEvidence::LookupFailed
                };
                let expected = match (call_ok, reference_refusal) {
                    (false, _) => V::Unknown(U::InstanceLookupFailed),
                    (true, Some(reason)) => V::Unknown(reason),
                    (true, None) => when_trusted,
                };
                assert_eq!(
                    instance_verdict(INSTANCE, &evidence),
                    expected,
                    "reference {reference:?}, instance {instance:?}, call ok {call_ok}"
                );
            }
        }
    }
    assert_eq!(
        instance_verdict(INSTANCE, &InstanceEvidence::NoAuthority),
        V::Unknown(U::NoInstanceAuthority)
    );
    assert_eq!(
        instance_verdict(INSTANCE, &InstanceEvidence::ReferenceUnavailable),
        V::Unknown(U::ReferenceUnavailable)
    );
}

/// Only `i-` and 8 or 17 lowercase hex digits is an instance ID; anything
/// else reads unknown even where an answer would read it dead.
#[test]
fn malformed_instance_ids_read_unknown_before_any_answer() {
    let cases = [
        ("i-0123abcd", true),
        ("i-0123456789abcdef0", true),
        ("i-0123ABCD", false),
        ("i-0123abc", false),
        ("i-0123abcde", false),
        ("i-0123456789abcdef", false),
        ("i-0123456789abcdef01", false),
        ("i-0123abcg", false),
        ("i-", false),
        ("", false),
        ("0123abcd", false),
        ("x-0123abcd", false),
        (" i-0123abcd", false),
        ("i-0123abcd ", false),
        ("i-0123abcd/x", false),
    ];
    for (id, well_formed) in cases {
        assert_eq!(well_formed_instance_id(id), well_formed, "{id:?}");
        // The reference listed running and `id` absent: dead when the ID is
        // well formed.
        let evidence = InstanceEvidence::Answered {
            listed: BTreeMap::from([(REFERENCE.to_owned(), "running".to_owned())]),
            reference: REFERENCE.to_owned(),
        };
        let expected = if well_formed {
            InstanceVerdict::Dead(DeadReason::Absent)
        } else {
            InstanceVerdict::Unknown(UnknownReason::MalformedInstanceId)
        };
        assert_eq!(instance_verdict(id, &evidence), expected, "{id:?}");
    }
}

fn all_hold() -> NodeVerdictInputs {
    NodeVerdictInputs {
        heartbeat: Heartbeat::Stale,
        kube: KubeEvidence::Absent {
            absent_for: WINDOW,
            direct_get: DirectGet::NotFound,
        },
        absence_window: WINDOW,
        recorded_instance: Some(InstanceVerdict::Dead(DeadReason::Terminated)),
    }
}

#[test]
fn every_condition_holding_reads_lost() {
    for dead in [DeadReason::Terminated, DeadReason::Absent] {
        let inputs = NodeVerdictInputs {
            recorded_instance: Some(InstanceVerdict::Dead(dead)),
            ..all_hold()
        };
        assert_eq!(
            node_verdict(&inputs),
            Verdict::Lost(LostReason::InstanceAndNodeGone)
        );
    }
}

/// Each condition of the node verdict missing on its own reads unknown with
/// its reason; the recorded instance's own unknown reason passes through.
#[test]
fn each_missing_condition_reads_unknown_with_its_reason() {
    use UnknownReason as U;
    let absent = |absent_for, direct_get| KubeEvidence::Absent {
        absent_for,
        direct_get,
    };
    let cases = [
        (
            NodeVerdictInputs {
                heartbeat: Heartbeat::Fresh,
                ..all_hold()
            },
            U::HeartbeatFresh,
        ),
        (
            NodeVerdictInputs {
                kube: KubeEvidence::NoRecordedIdentity,
                ..all_hold()
            },
            U::NoKubeIdentity,
        ),
        (
            NodeVerdictInputs {
                kube: KubeEvidence::MembershipUnknown,
                ..all_hold()
            },
            U::KubeMembershipUnknown,
        ),
        (
            NodeVerdictInputs {
                kube: KubeEvidence::Present,
                ..all_hold()
            },
            U::KubeNodePresent,
        ),
        (
            NodeVerdictInputs {
                kube: absent(WINDOW - Duration::from_secs(1), DirectGet::NotFound),
                ..all_hold()
            },
            U::KubeAbsenceSettling,
        ),
        (
            NodeVerdictInputs {
                kube: absent(WINDOW, DirectGet::Found),
                ..all_hold()
            },
            U::KubeNodeFound,
        ),
        (
            NodeVerdictInputs {
                kube: absent(WINDOW, DirectGet::Failed),
                ..all_hold()
            },
            U::KubeNodeLookupFailed,
        ),
        (
            NodeVerdictInputs {
                recorded_instance: None,
                ..all_hold()
            },
            U::NoRecordedInstance,
        ),
        (
            NodeVerdictInputs {
                recorded_instance: Some(InstanceVerdict::Live),
                ..all_hold()
            },
            U::InstanceLive,
        ),
        (
            NodeVerdictInputs {
                recorded_instance: Some(InstanceVerdict::Unknown(U::ReferenceMissing)),
                ..all_hold()
            },
            U::ReferenceMissing,
        ),
        (
            NodeVerdictInputs {
                recorded_instance: Some(InstanceVerdict::Unknown(U::NoInstanceAuthority)),
                ..all_hold()
            },
            U::NoInstanceAuthority,
        ),
    ];
    for (inputs, reason) in cases {
        assert_eq!(
            node_verdict(&inputs),
            Verdict::Unknown(reason),
            "{inputs:?}"
        );
    }
}
