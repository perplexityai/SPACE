use super::*;

#[test]
fn parses_canonical_storage_values() {
    assert_eq!(
        "organization_admin".parse::<UserRole>().unwrap(),
        UserRole::OrganizationAdmin
    );
    assert_eq!("member".parse::<UserRole>().unwrap(), UserRole::Member);
    "owner".parse::<UserRole>().unwrap_err();
}

#[test]
fn serializes_as_existing_wire_strings() {
    assert_eq!(
        serde_json::from_value::<UserRole>(serde_json::json!("organization_admin")).unwrap(),
        UserRole::OrganizationAdmin
    );
}
