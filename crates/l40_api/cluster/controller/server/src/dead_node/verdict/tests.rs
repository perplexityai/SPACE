use super::*;

const WINDOW: Duration = Duration::from_secs(600);

/// Every condition holds, for the given instance evidence.
fn all_hold(instance: InstanceEvidence) -> VerdictInputs {
    VerdictInputs {
        heartbeat: Heartbeat::Stale,
        kube: KubeEvidence::Absent {
            absent_for: WINDOW,
            direct_get: DirectGet::NotFound,
        },
        absence_window: WINDOW,
        instance,
    }
}

const EC2_GONE: InstanceEvidence = InstanceEvidence::Ec2 {
    node: InstanceLookup::NoLiveMatch,
    reference: ReferenceLookup::Running,
};

#[test]
fn every_condition_holding_reads_lost() {
    assert_eq!(
        verdict(&all_hold(EC2_GONE)),
        Verdict::Lost(LostReason::InstanceAndNodeGone)
    );
    assert_eq!(
        verdict(&all_hold(InstanceEvidence::KubeDelete)),
        Verdict::Lost(LostReason::NodeDeleted)
    );
}

/// Each condition missing in turn, with every other one holding, reads
/// unknown with that condition's reason, on both instance authorities.
#[test]
fn each_missing_condition_reads_unknown_with_its_reason() {
    use UnknownReason as U;
    type Edit = fn(&mut VerdictInputs);
    let cases: [(&str, Edit, UnknownReason); 7] = [
        (
            "heartbeat fresh",
            |i| i.heartbeat = Heartbeat::Fresh,
            U::HeartbeatFresh,
        ),
        (
            "no recorded Node",
            |i| i.kube = KubeEvidence::NoRecordedIdentity,
            U::NoKubeIdentity,
        ),
        (
            "reflector cold",
            |i| i.kube = KubeEvidence::MembershipUnknown,
            U::KubeMembershipUnknown,
        ),
        (
            "recorded uid listed",
            |i| i.kube = KubeEvidence::Present,
            U::KubeNodePresent,
        ),
        (
            "absent a second short of the window",
            |i| {
                i.kube = KubeEvidence::Absent {
                    absent_for: WINDOW - Duration::from_secs(1),
                    direct_get: DirectGet::NotFound,
                }
            },
            U::KubeAbsenceSettling,
        ),
        (
            "direct GET finds a Node by the name",
            |i| {
                i.kube = KubeEvidence::Absent {
                    absent_for: WINDOW,
                    direct_get: DirectGet::Found,
                }
            },
            U::KubeNodeFound,
        ),
        (
            "direct GET fails",
            |i| {
                i.kube = KubeEvidence::Absent {
                    absent_for: WINDOW,
                    direct_get: DirectGet::Failed,
                }
            },
            U::KubeNodeLookupFailed,
        ),
    ];
    for authority in [EC2_GONE, InstanceEvidence::KubeDelete] {
        for (case, edit, reason) in cases {
            let mut inputs = all_hold(authority);
            edit(&mut inputs);
            assert_eq!(
                verdict(&inputs),
                Verdict::Unknown(reason),
                "{case} with {authority:?}"
            );
        }
    }
}

/// The instance authority's half: EC2 that lists a live instance, errors, or
/// cannot show its reference live reads unknown; so does a cluster with no
/// authority, such as the `noop` terminator or Direct.
#[test]
fn instance_evidence_short_of_gone_reads_unknown() {
    use UnknownReason as U;
    let cases = [
        (
            InstanceEvidence::Ec2 {
                node: InstanceLookup::Live,
                reference: ReferenceLookup::Running,
            },
            U::InstanceLive,
        ),
        (
            InstanceEvidence::Ec2 {
                node: InstanceLookup::Failed,
                reference: ReferenceLookup::Running,
            },
            U::InstanceLookupFailed,
        ),
        (
            InstanceEvidence::Ec2 {
                node: InstanceLookup::NoLiveMatch,
                reference: ReferenceLookup::NotRunning,
            },
            U::ReferenceNotRunning,
        ),
        (
            InstanceEvidence::Ec2 {
                node: InstanceLookup::NoLiveMatch,
                reference: ReferenceLookup::Unavailable,
            },
            U::ReferenceUnavailable,
        ),
        (
            InstanceEvidence::Ec2 {
                node: InstanceLookup::NoLiveMatch,
                reference: ReferenceLookup::Failed,
            },
            U::ReferenceLookupFailed,
        ),
        (InstanceEvidence::NoAuthority, U::NoInstanceAuthority),
    ];
    for (instance, reason) in cases {
        assert_eq!(
            verdict(&all_hold(instance)),
            Verdict::Unknown(reason),
            "{instance:?}"
        );
    }
}
