use pplx_space_common_core::types::SandboxLifecycle;
use pplx_space_placement::StoredSandboxLifecycle;

use super::PublishedStatus;

fn unrecognized(text: &str) -> Option<PublishedStatus> {
    Some(PublishedStatus::Unrecognized(text.to_owned()))
}

#[test]
fn every_canonical_lifecycle_parses_as_itself() {
    for &lifecycle in StoredSandboxLifecycle::all() {
        assert_eq!(
            PublishedStatus::parse(Some(lifecycle.as_str())),
            Some(PublishedStatus::Known(lifecycle)),
            "{lifecycle}"
        );
    }
}

#[test]
fn padding_and_ascii_case_do_not_change_the_lifecycle() {
    let started = Some(PublishedStatus::Known(StoredSandboxLifecycle::Live(
        SandboxLifecycle::Started,
    )));
    for raw in [
        "STARTED",
        "Started",
        "  started  ",
        "\tStArTeD\r\n",
        "\u{3000}started",
    ] {
        assert_eq!(PublishedStatus::parse(Some(raw)), started, "{raw:?}");
    }
    assert_eq!(
        PublishedStatus::parse(Some(" Cleanup_Required ")),
        Some(PublishedStatus::Known(
            StoredSandboxLifecycle::CleanupRequired
        ))
    );
}

#[test]
fn absent_and_blank_statuses_are_none() {
    for raw in [None, Some(""), Some("   "), Some("\t\n")] {
        assert_eq!(PublishedStatus::parse(raw), None, "{raw:?}");
    }
}

/// Unknown text is kept trimmed, with its case. That includes text outside
/// ASCII whose Unicode lower case is a lifecycle: `İ` lower-cases to `i`
/// and the Kelvin sign to `k`.
#[test]
fn unknown_statuses_are_unrecognized_and_trimmed() {
    for (raw, text) in [
        ("bogus", "bogus"),
        (" Running ", "Running"),
        ("startedx", "startedx"),
        ("sta rted", "sta rted"),
        ("cleanup-required", "cleanup-required"),
        ("FAİLED", "FAİLED"),
        (" QUARANTİNED ", "QUARANTİNED"),
    ] {
        assert_eq!(
            PublishedStatus::parse(Some(raw)),
            unrecognized(text),
            "{raw:?}"
        );
    }
}

#[test]
fn only_a_known_status_reports_a_lifecycle() {
    let failed = PublishedStatus::parse(Some("FAILED")).unwrap();
    assert!(failed.is(SandboxLifecycle::Failed));
    assert!(!failed.is(SandboxLifecycle::Started));
    assert_eq!(failed.as_str(), "failed");

    let deleted = PublishedStatus::parse(Some("deleted")).unwrap();
    assert_eq!(deleted.lifecycle(), Some(StoredSandboxLifecycle::Deleted));
    assert!(
        SandboxLifecycle::all()
            .iter()
            .all(|live| !deleted.is(*live))
    );

    let unknown = PublishedStatus::parse(Some("FAİLED")).unwrap();
    assert_eq!(unknown.lifecycle(), None);
    assert!(!unknown.is(SandboxLifecycle::Failed));
    assert_eq!(unknown.as_str(), "FAİLED");
}
