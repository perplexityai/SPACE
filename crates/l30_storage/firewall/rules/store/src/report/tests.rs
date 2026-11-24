use serde_json::json;

use super::*;

fn slot(slot_id: u32, outcome: SlotOutcome) -> SlotEvidence {
    SlotEvidence {
        slot_id,
        slot_generation: u64::from(slot_id) + 1,
        outcome,
    }
}

fn incomplete(gaps: Vec<CensusGap>, extra_namespaces: u64) -> Result<Report, InvalidReport> {
    let census = Census::Incomplete {
        gaps,
        extra_namespaces,
    };
    let revision = RevisionId(uuid::Uuid::nil());
    Report::new(7, 12, revision, 1, census, Vec::new())
}

/// Every gap and slot outcome as the version 1 document stores it, in its
/// canonical order, read back by the derived `Deserialize`. Catches a renamed
/// field, tag or value, which a retained document would then compare as a
/// conflict, and a gap order other than host-wide first, then by slot.
#[test]
fn every_document_variant_is_written_as_frozen_and_reads_back() {
    let census = Census::Incomplete {
        gaps: vec![
            CensusGap::Unread { slot_id: 1 },
            CensusGap::ListingFailed,
            CensusGap::Missing { slot_id: 0 },
            CensusGap::Unowned,
            CensusGap::InventoryFailed,
        ],
        extra_namespaces: 3,
    };
    let reasons = [
        UnknownReason::Ineligible,
        UnknownReason::NamespaceMissing,
        UnknownReason::NamespaceUnreadable,
        UnknownReason::TableUnreadable,
        UnknownReason::Unconfirmed,
    ];
    let mut slots = [
        SlotOutcome::Verified,
        SlotOutcome::Applying,
        SlotOutcome::Failed(Failure::RulesMismatch),
    ]
    .into_iter()
    .chain(reasons.map(SlotOutcome::Unknown))
    .zip(0..)
    .map(|(outcome, slot_id)| slot(slot_id, outcome))
    .collect::<Vec<_>>();
    slots.reverse();
    let revision = RevisionId(uuid::Uuid::nil());
    let report = Report::new(7, 12, revision, 1, census, slots.clone()).unwrap();
    let unknown = |slot_id: u32, reason: &str| {
        json!({
            "outcome": "unknown",
            "slot_id": slot_id,
            "slot_generation": slot_id + 1,
            "reason": reason,
        })
    };
    let slots_json = json!([
        {"outcome": "verified", "slot_id": 0, "slot_generation": 1},
        {"outcome": "applying", "slot_id": 1, "slot_generation": 2},
        {"outcome": "failed", "slot_id": 2, "slot_generation": 3, "reason": "rules_mismatch"},
        unknown(3, "ineligible"),
        unknown(4, "namespace_missing"),
        unknown(5, "namespace_unreadable"),
        unknown(6, "table_unreadable"),
        unknown(7, "unconfirmed"),
    ]);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&report.document).unwrap(),
        json!({
            "schema_version": 1,
            "round": 7,
            "assignment_generation": 12,
            "revision_id": "00000000-0000-0000-0000-000000000000",
            "manager_epoch": 1,
            "census": "incomplete",
            "census_gaps": [
                {"gap": "inventory_failed"},
                {"gap": "unowned"},
                {"gap": "listing_failed"},
                {"gap": "missing", "slot_id": 0},
                {"gap": "unread", "slot_id": 1},
            ],
            "extra_namespaces": 3,
            "slots": slots_json,
        })
    );
    let complete = Report::new(7, 12, revision, 1, Census::Complete, slots).unwrap();
    for report in [report, complete] {
        let read = serde_json::from_str::<ReportV1>(&report.document).unwrap();
        assert_eq!(serde_json::to_string(&read).unwrap(), report.document);
    }
}

/// Extra namespaces alone make a census incomplete, up to the cap. Catches an
/// incomplete census that must list a gap, and a cap that refuses its own
/// value or admits the next.
#[test]
fn extra_namespaces_alone_are_incomplete_up_to_the_cap() {
    for extra in [1, MAX_EXTRA_NAMESPACES] {
        assert!(incomplete(Vec::new(), extra).is_ok(), "{extra}");
    }
    assert_eq!(
        incomplete(Vec::new(), MAX_EXTRA_NAMESPACES + 1),
        Err(InvalidReport::TooManyExtraNamespaces(
            MAX_EXTRA_NAMESPACES + 1
        ))
    );
    assert_eq!(incomplete(Vec::new(), 0), Err(InvalidReport::NoGaps));
}
