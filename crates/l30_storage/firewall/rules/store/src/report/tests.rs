use super::*;

/// Catches a document the derived `Deserialize` doesn't read back, for every
/// gap and slot outcome: a renamed field or tag on one side only.
#[test]
fn every_document_reads_back_as_written() {
    let slot = |slot_id, outcome| SlotEvidence {
        slot_id,
        slot_generation: u64::from(slot_id) + 1,
        outcome,
    };
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
    let slots = [
        SlotOutcome::Verified,
        SlotOutcome::Applying,
        SlotOutcome::Failed(Failure::RulesMismatch),
    ]
    .into_iter()
    .chain(reasons.map(SlotOutcome::Unknown))
    .zip(0..)
    .map(|(outcome, slot_id)| slot(slot_id, outcome))
    .collect::<Vec<_>>();
    let revision = RevisionId(uuid::Uuid::now_v7());
    for census in [Census::Complete, census] {
        let report = Report::new(7, 12, revision, 1, census, slots.clone()).unwrap();
        let read = serde_json::from_str::<ReportV1>(&report.document).unwrap();
        assert_eq!(serde_json::to_string(&read).unwrap(), report.document);
    }
}
