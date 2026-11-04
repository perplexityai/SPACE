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
