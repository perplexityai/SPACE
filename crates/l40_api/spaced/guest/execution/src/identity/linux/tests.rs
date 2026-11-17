use super::*;
use crate::IdentityPolicy;
use nix::unistd::{Gid, Uid};

#[test]
fn contained_policy_cannot_request_root_or_another_account() {
    let current = Uid::effective().as_raw();
    let gid = Gid::effective().as_raw();
    // Admission must reject an otherwise valid non-root account before worker
    // spawn, independently of whether setresuid would happen to fail later.
    for (uid, account_gid, allowed) in [
        (current, gid, current != 0),
        (0, gid, false),
        (if current == 1 { 2 } else { 1 }, gid, false),
        (current, gid.wrapping_add(1), false),
    ] {
        let account = GuestAccount {
            name: GuestUserName::parse("user").unwrap(),
            home: "/workspace".into(),
            shell: "/bin/false".into(),
            uid,
            gid: account_gid,
            supplementary_groups: Vec::new(),
        };
        assert_eq!(
            account.validate_policy(IdentityPolicy::Contained).is_ok(),
            allowed,
        );
    }
}
