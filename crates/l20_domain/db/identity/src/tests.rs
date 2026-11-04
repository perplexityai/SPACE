use super::DatabaseIdentity;

#[test]
fn role_names_match_the_managed_postgres_logins() {
    assert_eq!(DatabaseIdentity::SpaceUser.as_str(), "space_user");
    assert_eq!(DatabaseIdentity::SpaceCreds.as_str(), "space_creds");
}

#[test]
fn display_forwards_to_the_canonical_role_name() {
    for identity in [DatabaseIdentity::SpaceUser, DatabaseIdentity::SpaceCreds] {
        assert_eq!(identity.to_string(), identity.as_str());
    }
}
