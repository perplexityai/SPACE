use super::*;

const CURRENT: &str = "00000001";
const OTHER: &str = "00000002";

fn version(digits: &str) -> PartVersion {
    digits.parse().unwrap()
}

/// A slot's stamps: `OTHER` for the parts in `other`, the given reading for
/// the parts in `unread`, and `CURRENT` for the rest.
fn stamps(other: &[Part], unread: &[(Part, StampReading)]) -> NamespaceStamps {
    PerPart::from_fn(
        |part| match unread.iter().find(|(named, _)| *named == part) {
            Some((_, reading)) => *reading,
            None if other.contains(&part) => StampReading::Version(version(OTHER)),
            None => StampReading::Version(version(CURRENT)),
        },
    )
}

/// A slot's rules: `state` for the parts in `parts`, the proxy as given, and
/// `rest` for every other part.
fn rules(parts: &[Part], state: PartState, rest: PartState, proxy: Option<PartState>) -> SlotRules {
    PerPart::from_fn(|part| match part {
        Part::Proxy => proxy,
        part if parts.contains(&part) => Some(state),
        _ => Some(rest),
    })
}

#[test]
fn startup_compares_each_parts_stamp_with_this_binarys_version() {
    use PartState::{Current, Stale, Unknown};
    use StampReading::{NoStamp, NoTable};
    let current = PerPart::from_fn(|_| version(CURRENT));
    for (stamps, current, expected) in [
        (
            Some(stamps(&[], &[])),
            Some(&current),
            rules(&[], Current, Current, Some(Current)),
        ),
        (
            Some(stamps(&[Part::Nat, Part::Proxy], &[])),
            Some(&current),
            rules(&[Part::Nat], Stale, Current, Some(Stale)),
        ),
        // An absent proxy table is not unknown; an unstamped part is.
        (
            Some(stamps(
                &[],
                &[(Part::PackagedPolicy, NoStamp), (Part::Proxy, NoTable)],
            )),
            Some(&current),
            rules(&[Part::PackagedPolicy], Unknown, Current, None),
        ),
        // Proxy rules without a usable stamp are present, so unknown, and
        // any other part whose table is missing is unknown too.
        (
            Some(stamps(&[], &[(Part::Nat, NoTable), (Part::Proxy, NoStamp)])),
            Some(&current),
            rules(&[Part::Nat], Unknown, Current, Some(Unknown)),
        ),
        // A namespace startup did not visit shows nothing, not even absence.
        (
            None,
            Some(&current),
            rules(&[], Unknown, Unknown, Some(Unknown)),
        ),
        // Without this binary's versions, no stamp can be compared.
        (
            Some(stamps(&[], &[])),
            None,
            rules(&[], Unknown, Unknown, Some(Unknown)),
        ),
    ] {
        let versions = RuleVersions::at_startup([(7, stamps.clone())], current);
        assert_eq!(versions.slot(7), Some(expected), "{stamps:?}");
    }
}

#[derive(Debug)]
enum Install {
    /// A create or reset, and whether it completed.
    Base(bool),
    /// A net-gateway launch, and whether it completed.
    Proxy(bool),
}

#[test]
fn installs_change_only_the_parts_they_rebuild() {
    use PartState::{Current, Stale, Unknown};
    let current = PerPart::from_fn(|_| version(CURRENT));
    let versions = RuleVersions::at_startup(
        [(7, Some(stamps(&[Part::Nat, Part::Proxy], &[])))],
        Some(&current),
    );
    for (install, expected) in [
        (
            Install::Proxy(true),
            rules(&[Part::Nat], Stale, Current, Some(Current)),
        ),
        (Install::Base(true), rules(&[], Current, Current, None)),
        (
            Install::Proxy(false),
            rules(&[], Current, Current, Some(Unknown)),
        ),
        (
            Install::Base(false),
            rules(&[], Unknown, Unknown, Some(Unknown)),
        ),
    ] {
        match install {
            Install::Base(succeeded) => versions.base_installed(7, succeeded),
            Install::Proxy(succeeded) => versions.proxy_installed(7, succeeded),
        }
        assert_eq!(versions.slot(7), Some(expected), "{install:?}");
    }
    // A launch in a slot startup did not record leaves its other parts unknown.
    versions.proxy_installed(8, true);
    assert_eq!(
        versions.slot(8),
        Some(rules(&[], Unknown, Unknown, Some(Current)))
    );
}

#[test]
fn counts_each_slot_once_by_state_and_each_stale_part() {
    use StampReading::{NoStamp, NoTable};
    let current = PerPart::from_fn(|_| version(CURRENT));
    let versions = RuleVersions::at_startup(
        [
            // Current, with no proxy table: neither stale nor unknown.
            (1, Some(stamps(&[], &[(Part::Proxy, NoTable)]))),
            (2, Some(stamps(&[Part::Nat], &[(Part::Proxy, NoTable)]))),
            // Two stale parts: one stale slot, counted under both parts.
            (3, Some(stamps(&[Part::Nat, Part::Proxy], &[]))),
            (4, Some(stamps(&[], &[(Part::PackagedPolicy, NoStamp)]))),
            // A stale part outweighs an unknown one.
            (
                5,
                Some(stamps(&[Part::Forward], &[(Part::PackagedPolicy, NoStamp)])),
            ),
        ],
        Some(&current),
    );
    // Slot 6 has no record, so it reads unknown.
    let counts = versions.counts([
        (1, "ready"),
        (2, "ready"),
        (6, "ready"),
        (3, "leased"),
        (4, "leased"),
        (5, "dirty"),
    ]);
    let expected = |stale, parts: &[Part], unknown| StaleCounts {
        stale,
        stale_parts: PerPart::from_fn(|part| u32::from(parts.contains(&part))),
        unknown,
    };
    assert_eq!(counts["ready"], expected(1, &[Part::Nat], 1));
    assert_eq!(counts["leased"], expected(1, &[Part::Nat, Part::Proxy], 1));
    assert_eq!(counts["dirty"], expected(1, &[Part::Forward], 0));
    assert_eq!(counts.len(), 3);
}
