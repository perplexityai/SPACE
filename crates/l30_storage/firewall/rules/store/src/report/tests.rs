use serde_json::json;

use super::*;

fn slot(slot_id: u32, outcome: SlotOutcome) -> SlotEvidence {
    SlotEvidence {
        slot_id,
        slot_generation: u64::from(slot_id) + 1,
        outcome,
    }
}

fn incomplete(
    gaps: Vec<CensusGap>,
    extra_namespaces: u64,
    slots: Vec<SlotEvidence>,
) -> Result<Report, InvalidReport> {
    let census = Census::Incomplete {
        gaps,
        extra_namespaces,
    };
    let revision = RevisionId(uuid::Uuid::nil());
    Report::new(7, 12, revision, 1, census, slots)
}

/// Every gap and slot outcome as the version 1 document stores it, in its
/// canonical order, read back by the derived `Deserialize`. Catches a renamed
/// field, tag or value, which a retained document would then compare as a
/// conflict, and a gap order other than host-wide first, then by slot.
#[test]
fn every_document_variant_is_written_as_frozen_and_reads_back() {
    let census = Census::Incomplete {
        gaps: vec![
            CensusGap::Missing { slot_id: 4 },
            CensusGap::ListingFailed,
            CensusGap::Unread { slot_id: 3 },
            CensusGap::Unowned,
            CensusGap::InventoryFailed,
        ],
        extra_namespaces: 3,
    };
    // An UNREAD gap's slot below a MISSING gap's, so sorting by kind first
    // would order them differently.
    let reasons = [
        UnknownReason::Unconfirmed,
        UnknownReason::NamespaceMissing,
        UnknownReason::Ineligible,
        UnknownReason::NamespaceUnreadable,
        UnknownReason::TableUnreadable,
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
        unknown(3, "unconfirmed"),
        unknown(4, "namespace_missing"),
        unknown(5, "ineligible"),
        unknown(6, "namespace_unreadable"),
        unknown(7, "table_unreadable"),
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
                {"gap": "unread", "slot_id": 3},
                {"gap": "missing", "slot_id": 4},
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
        assert!(incomplete(Vec::new(), extra, Vec::new()).is_ok(), "{extra}");
    }
    assert_eq!(
        incomplete(Vec::new(), MAX_EXTRA_NAMESPACES + 1, Vec::new()),
        Err(InvalidReport::TooManyExtraNamespaces(
            MAX_EXTRA_NAMESPACES + 1
        ))
    );
    assert_eq!(
        incomplete(Vec::new(), 0, Vec::new()),
        Err(InvalidReport::NoGaps)
    );
}

/// Every slot outcome against each slot gap: only MISSING on
/// NAMESPACE_MISSING, and UNREAD on UNCONFIRMED or NAMESPACE_UNREADABLE, are
/// accepted. Catches a gap that refuses a slot a worker reports for it, or
/// accepts one that contradicts it.
#[test]
fn a_slot_gap_fits_only_its_unknown_reasons() {
    use UnknownReason as R;
    let outcomes = [
        SlotOutcome::Verified,
        SlotOutcome::Applying,
        SlotOutcome::Failed(Failure::RulesMismatch),
        SlotOutcome::Unknown(R::Ineligible),
        SlotOutcome::Unknown(R::NamespaceMissing),
        SlotOutcome::Unknown(R::NamespaceUnreadable),
        SlotOutcome::Unknown(R::TableUnreadable),
        SlotOutcome::Unknown(R::Unconfirmed),
    ];
    for outcome in outcomes {
        for gap in [
            CensusGap::Missing { slot_id: 0 },
            CensusGap::Unread { slot_id: 0 },
        ] {
            let fits = matches!(
                (gap, outcome),
                (
                    CensusGap::Missing { .. },
                    SlotOutcome::Unknown(R::NamespaceMissing)
                ) | (
                    CensusGap::Unread { .. },
                    SlotOutcome::Unknown(R::Unconfirmed | R::NamespaceUnreadable)
                )
            );
            let report = incomplete(vec![gap], 0, vec![slot(0, outcome)]);
            assert_eq!(
                report.err(),
                (!fits).then_some(InvalidReport::GapContradictsSlot(0)),
                "{gap:?} on {outcome:?}"
            );
        }
    }
}
