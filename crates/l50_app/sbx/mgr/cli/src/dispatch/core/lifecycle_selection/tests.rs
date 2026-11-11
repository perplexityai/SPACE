use super::*;

#[test]
fn exact_mutation_requests_preserve_the_template_precondition() {
    let id = SandboxId::new().to_string();
    for mutation in [
        Mutation::Stop {
            immediate_kill: false,
        },
        Mutation::Delete,
    ] {
        for template in [None, Some("selected-template".to_string())] {
            let (actual_id, actual_template, blocking) =
                match mutation.request(id.clone(), template.clone(), true) {
                    MutationRequest::Stop(request) => (
                        request.sandbox_id,
                        request.template_filter,
                        request.blocking,
                    ),
                    MutationRequest::Delete(request) => (
                        request.sandbox_id,
                        request.template_filter,
                        request.blocking,
                    ),
                };
            assert_eq!(actual_id, id);
            assert_eq!(actual_template, template);
            assert!(blocking);
        }
    }
}

#[test]
fn selection_preserves_suffix_ambiguity_before_template_filter_and_unknown_metadata() {
    let first = CleanupTargetObservation {
        sandbox_id: "019d84bd-280d-7331-99c6-b63377f3a3a2".into(),
        template: Some("seed".into()),
        metadata_valid: true,
    };
    for (selector, matches) in [
        ("77f3", true),
        ("019d84bd-280d", true),
        ("019d84bd", false),
        ("aabb", false),
    ] {
        assert_eq!(
            matching_id(&first, Some(&SandboxSelector::parse(selector).unwrap()))
                .unwrap()
                .is_some(),
            matches
        );
    }
    for has_template in [false, true] {
        for valid in [false, true] {
            let target = CleanupTargetObservation {
                metadata_valid: valid,
                template: has_template.then(|| "seed".into()),
                ..first.clone()
            };
            for filter in [None, Some("seed"), Some("other")] {
                let result = eligible(&target, filter);
                if !valid {
                    assert!(result.is_err());
                } else {
                    assert_eq!(
                        result.unwrap(),
                        filter.is_none() || (has_template && filter == Some("seed"))
                    );
                }
            }
        }
    }
    let mut selected = None;
    unique_target(&mut selected, first.clone()).unwrap();
    unique_target(&mut selected, first.clone()).unwrap();
    let second = CleanupTargetObservation {
        sandbox_id: "019d84bd-280d-7331-99c6-b63377f3a3a3".into(),
        template: Some("other".into()),
        ..first
    };
    assert!(unique_target(&mut selected, second).is_err());
}
