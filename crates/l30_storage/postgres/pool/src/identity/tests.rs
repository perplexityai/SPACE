use super::managed_password;
use crate::{SPACE_DB_CREDS_PASSWORD_ENV, SPACE_DB_USER_PASSWORD_ENV};
use pplx_space_db_identity::DatabaseIdentity;

#[test]
fn projected_passwords_pass_through() {
    for identity in [DatabaseIdentity::SpaceUser, DatabaseIdentity::SpaceCreds] {
        let password = managed_password(identity, Some("pw".to_owned())).unwrap();

        assert_eq!(password, "pw");
    }
}

#[test]
fn missing_password_error_names_the_identity_and_env_var() {
    for (identity, env_var) in [
        (DatabaseIdentity::SpaceUser, SPACE_DB_USER_PASSWORD_ENV),
        (DatabaseIdentity::SpaceCreds, SPACE_DB_CREDS_PASSWORD_ENV),
    ] {
        for absent in [None, Some(String::new())] {
            let error = managed_password(identity, absent).unwrap_err();
            let message = error.to_string();

            assert!(message.contains(env_var), "{message}");
            assert!(message.contains(identity.as_str()), "{message}");
        }
    }
}
