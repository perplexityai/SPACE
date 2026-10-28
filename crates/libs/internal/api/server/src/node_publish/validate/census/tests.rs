use super::*;

#[test]
fn complete_empty_and_invalid_censuses_keep_distinct_pruning_permissions() {
    let id = SandboxId::from_uuid(uuid::Uuid::from_u128(1));
    let present = PublishedSandboxState {
        sandbox_id: id.to_string(),
        present: true,
        ..Default::default()
    };
    let absent = PublishedSandboxState {
        present: false,
        ..present.clone()
    };
    let malformed = PublishedSandboxState {
        sandbox_id: "invalid".to_owned(),
        ..present.clone()
    };
    let malformed_absence = PublishedSandboxState {
        present: false,
        ..malformed.clone()
    };
    for complete in [false, true] {
        for (states, expected_ids) in [
            (vec![], Some(vec![])),
            (vec![absent.clone()], Some(vec![])),
            (
                vec![present.clone(), absent.clone(), present.clone()],
                Some(vec![id, id]),
            ),
            (vec![present.clone(), malformed.clone()], None),
            (vec![present.clone(), malformed_absence.clone()], None),
        ] {
            let (census, _) = SandboxCensus::from_proto(&states, complete);
            assert_eq!(census.claimed_complete(), complete);
            let expected = complete.then_some(expected_ids).flatten();
            assert_eq!(census.pruning_ids(), expected.as_deref());
        }
    }
}
