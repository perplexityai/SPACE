use super::*;
use pplx_space_common_core::types::{DEPLOY_SANDBOX_OWNER, DEV_SANDBOX_OWNER};

#[test]
fn local_sandbox_owner_for_effective_identity_maps_core_cases() {
    let cases = [
        (0, Some("deploy"), ROOT_SANDBOX_OWNER.clone()),
        (1000, Some("deploy"), DEPLOY_SANDBOX_OWNER.clone()),
        (1001, Some("dev"), DEV_SANDBOX_OWNER.clone()),
    ];

    for (uid, username, expected) in cases {
        assert_eq!(
            local_sandbox_owner_for_effective_identity(uid, username).unwrap(),
            expected
        );
    }

    for username in [
        "root", "builder", "tmplmgr", "orphaned", "alice", "bad name",
    ] {
        local_sandbox_owner_for_effective_identity(1002, Some(username)).unwrap_err();
    }
    local_sandbox_owner_for_effective_identity(1002, None).unwrap_err();
}

#[test]
fn explicit_create_user_accepts_only_supported_local_owners() {
    for owner in [
        ROOT_SANDBOX_OWNER.as_str(),
        DEPLOY_SANDBOX_OWNER.as_str(),
        DEV_SANDBOX_OWNER.as_str(),
    ] {
        assert_eq!(
            parse_cli_local_sandbox_owner(owner).unwrap().as_str(),
            owner
        );
    }

    for owner in [
        "uid-1000@local",
        "james@local",
        "james@example.test",
        "user:00000000-0000-0000-0000-000000000020",
        "builder@local",
        "tmplmgr@local",
    ] {
        parse_cli_local_sandbox_owner(owner).unwrap_err();
    }
}
