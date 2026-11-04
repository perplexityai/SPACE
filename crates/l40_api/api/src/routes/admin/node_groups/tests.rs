use super::*;
use pplx_space_common_core::types::NodeGroupId;

#[test]
fn parse_group_spec_accepts_bare_uuid_and_uuid_colon_action() {
    let id = NodeGroupId::new();

    let (parsed_id, action) = parse_group_spec(&id.to_string()).unwrap();
    assert_eq!(parsed_id, id);
    assert!(action.is_none());

    let (parsed_id, action) = parse_group_spec(&format!("{id}:assign-org")).unwrap();
    assert_eq!(parsed_id, id);
    assert!(matches!(action, Some(NodeGroupAction::AssignOrg)));

    let (_, action) = parse_group_spec(&format!("{id}:set-default")).unwrap();
    assert_eq!(action, Some(NodeGroupAction::SetDefault));
}

#[test]
fn parse_group_spec_rejects_unknown_action_with_not_found() {
    let id = Uuid::now_v7();
    let err = parse_group_spec(&format!("{id}:bogus")).unwrap_err();
    assert_eq!(err.0, StatusCode::NOT_FOUND);
}

#[test]
fn parse_group_spec_rejects_bad_uuid_with_bad_request() {
    let err = parse_group_spec("not-a-uuid:assign-org").unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

#[test]
fn parse_bare_group_id_rejects_action_suffix() {
    let id = NodeGroupId::new();
    let err = parse_bare_group_id(&format!("{id}:assign-org")).unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert_eq!(parse_bare_group_id(&id.to_string()).unwrap(), id);
}

#[test]
fn json_body_is_required_for_admin_writes() {
    // An accidental empty `:assign-org` POST must not be interpreted as
    // an org release.
    let err = parse_json_body::<pplx_space_common_core::types::admin::AssignOrgRequest>(
        b"",
        "assign-org",
    )
    .unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}

#[test]
fn assign_org_body_rejects_unknown_fields() {
    // A typoed key (`org-id`) must surface as 400, not silently release
    // the group to generic.
    let err = parse_json_body::<pplx_space_common_core::types::admin::AssignOrgRequest>(
        br#"{"org-id": "00000000-0000-0000-0000-000000000001"}"#,
        "assign-org",
    )
    .unwrap_err();
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
}
