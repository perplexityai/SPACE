use super::*;

#[test]
fn identities_are_disjoint_reversible_and_reject_non_sandbox_peers() {
    let mut seen = std::collections::HashSet::new();
    for slot in [0, 1, 999, MAX_LINUX_NETWORK_SLOT] {
        let owner = LinuxUid::new(SANDBOX_UID_BASE + slot);
        let peer = gateway_uid(owner).unwrap();
        assert_ne!(owner, peer);
        assert!(seen.insert(owner));
        assert!(seen.insert(peer));
        assert_eq!(sandbox_uid(owner), Some(owner));
        assert_eq!(sandbox_uid(peer), Some(owner));
        assert_eq!(gateway_uid(peer), None);
    }
    for uid in [
        0,
        SANDBOX_UID_BASE - 1,
        SANDBOX_UID_BASE + MAX_LINUX_NETWORK_SLOT + 1,
        GATEWAY_UID_BASE - 1,
        GATEWAY_UID_BASE + MAX_LINUX_NETWORK_SLOT + 1,
        u32::MAX,
    ] {
        assert_eq!(sandbox_uid(LinuxUid::new(uid)), None);
    }
}
