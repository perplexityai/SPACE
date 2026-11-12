use super::*;

fn id(value: &str) -> ClientId {
    ClientId::parse(value).expect("valid id")
}

/// The record follows the tracked pod: a different pod sets it to that pod's
/// label or clears it, a labeled view of the same pod repairs it, and an
/// unlabeled view of the same pod, or no pod at all, leaves it alone.
#[test]
fn the_record_follows_the_tracked_pod() {
    let alpha = id("alpha");
    let beta = id("beta");
    /// The tracked pod's UID before and after, the observed pod's label, the
    /// current record, and what the record should become.
    type Case<'a> = (
        Option<&'a str>,
        Option<&'a str>,
        Option<&'a ClientId>,
        Option<&'a ClientId>,
        Option<Option<ClientId>>,
    );
    let cases: [Case<'_>; 10] = [
        // A different pod, from none or from another pod.
        (
            None,
            Some("uid-1"),
            Some(&alpha),
            None,
            Some(Some(alpha.clone())),
        ),
        (
            Some("uid-0"),
            Some("uid-1"),
            Some(&alpha),
            Some(&beta),
            Some(Some(alpha.clone())),
        ),
        (Some("uid-0"), Some("uid-1"), None, Some(&beta), Some(None)),
        // A different pod whose label is already recorded.
        (
            Some("uid-0"),
            Some("uid-1"),
            Some(&alpha),
            Some(&alpha),
            None,
        ),
        // A different unlabeled pod when nothing is recorded.
        (None, Some("uid-1"), None, None, None),
        // The same pod.
        (
            Some("uid-1"),
            Some("uid-1"),
            Some(&alpha),
            Some(&beta),
            Some(Some(alpha.clone())),
        ),
        (
            Some("uid-1"),
            Some("uid-1"),
            Some(&alpha),
            None,
            Some(Some(alpha.clone())),
        ),
        (
            Some("uid-1"),
            Some("uid-1"),
            Some(&alpha),
            Some(&alpha),
            None,
        ),
        (Some("uid-1"), Some("uid-1"), None, Some(&beta), None),
        // No pod tracked after the write.
        (Some("uid-1"), None, Some(&alpha), Some(&beta), None),
    ];
    for (case, (previous, pod, label, record, expected)) in cases.into_iter().enumerate() {
        assert_eq!(
            attribution_update("started", previous, pod, label, record),
            expected,
            "case {case}"
        );
    }
}

/// A sandbox no activation can be creating a runtime for is never
/// re-attributed, whatever pod is observed.
#[test]
fn unattributed_lifecycles_are_never_re_attributed() {
    let alpha = id("alpha");
    for lifecycle in UNATTRIBUTED_LIFECYCLES {
        assert_eq!(
            attribution_update(lifecycle, None, Some("uid-1"), Some(&alpha), None),
            None,
            "{lifecycle}"
        );
    }
    assert_eq!(
        attribution_update("started", None, Some("uid-1"), Some(&alpha), None),
        Some(Some(alpha))
    );
}
