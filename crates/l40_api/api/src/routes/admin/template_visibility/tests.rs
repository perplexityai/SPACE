use super::*;
use pplx_space_common_core::types::OrgId;
use uuid::Uuid;

/// Truth table for the no-op short-circuit. The supplied-equal-owner row
/// is the regression case: a desired-state PUT repeating the current
/// owner must not fall through to the in-use guards and 409.
#[test]
fn visibility_noop_covers_desired_state_puts() {
    let org_a = Some(Uuid::from_u128(0x1));
    let org_b = Some(Uuid::from_u128(0x3));
    let request = |visibility, org_id| SetTemplateVisibilityRequest { visibility, org_id };

    // Same visibility, no owner supplied → no-op.
    assert!(is_visibility_noop(
        &request(TemplateVisibility::OrgSpecific, None),
        TemplateVisibility::OrgSpecific,
        org_a.map(OrgId::from_uuid),
    ));
    // Same visibility, supplied owner equals current → no-op (the
    // idempotent desired-state PUT).
    assert!(is_visibility_noop(
        &request(TemplateVisibility::OrgSpecific, org_a.map(OrgId::from_uuid)),
        TemplateVisibility::OrgSpecific,
        org_a.map(OrgId::from_uuid),
    ));
    // Differing supplied owner is not a no-op (the reassignment guard
    // rejects it before this check, but the predicate must not lie).
    assert!(!is_visibility_noop(
        &request(TemplateVisibility::OrgSpecific, org_b.map(OrgId::from_uuid)),
        TemplateVisibility::OrgSpecific,
        org_a.map(OrgId::from_uuid),
    ));
    // Visibility change is never a no-op.
    assert!(!is_visibility_noop(
        &request(TemplateVisibility::Generic, None),
        TemplateVisibility::OrgSpecific,
        org_a.map(OrgId::from_uuid),
    ));
    assert!(!is_visibility_noop(
        &request(TemplateVisibility::OrgSpecific, org_a.map(OrgId::from_uuid)),
        TemplateVisibility::Generic,
        org_a.map(OrgId::from_uuid),
    ));
}

#[test]
fn template_spec_requires_the_visibility_action() {
    parse_template_spec("seed:visibility").unwrap();

    let err = parse_template_spec("seed").unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);

    let err = parse_template_spec("seed:rename").unwrap_err();
    assert_eq!(err.0, StatusCode::NOT_FOUND);

    let err = parse_template_spec("../seed:visibility").unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}
