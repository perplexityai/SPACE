use super::*;
use uuid::Uuid;

fn org(n: u128) -> OrgId {
    OrgId::from_uuid(Uuid::from_u128(n))
}

fn group(name: &str, org_n: u128, desired: i32, age: i64) -> Group {
    Group {
        name: name.to_string(),
        org_id: org(org_n),
        pool: "workers-12".to_string(),
        desired_nodes: desired,
        created_at: time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(age),
        cache_kms_key_arn: None,
    }
}

fn node(name: &str, group: Option<&str>) -> PoolNode {
    PoolNode {
        name: name.to_string(),
        pool: "workers-12".to_string(),
        group: group.map(str::to_string),
        unassigned_taint: group.is_none(),
        leaving: false,
        registered: true,
        instance_id: Some(format!("i-{name}")),
        encryption: EncryptionClaim {
            boot_id: Some("boot-1".to_string()),
            ready: true,
            ..EncryptionClaim::default()
        },
    }
}

const KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/0000-tenant";

fn encrypted_group(name: &str, org_n: u128, desired: i32) -> Group {
    Group {
        cache_kms_key_arn: Some(KEY.to_string()),
        ..group(name, org_n, desired, 0)
    }
}

/// A free node reserved for `group` at boot-1.
fn reserved(name: &str, group: &str) -> PoolNode {
    let mut node = node(name, None);
    node.encryption.group = Some(group.to_string());
    node.encryption.claim_boot = Some("boot-1".to_string());
    node
}

#[test]
fn an_empty_pool_grows_to_what_groups_need() {
    let actions = plan(&[group("t-a", 1, 2, 0)], &[]);
    assert_eq!(
        actions,
        vec![Action::Grow {
            pool: "workers-12".into(),
            nodes: 2
        }]
    );
}

#[test]
fn a_new_node_is_fenced_and_claimed_by_the_oldest_short_group() {
    let mut fresh = node("n1", None);
    fresh.unassigned_taint = false;
    let actions = plan(&[group("t-b", 2, 1, 10), group("t-a", 1, 1, 0)], &[fresh]);
    assert_eq!(
        actions,
        vec![
            Action::Fence { node: "n1".into() },
            Action::Claim {
                node: "n1".into(),
                group: "t-a".into(),
                org_id: org(1)
            },
            Action::Grow {
                pool: "workers-12".into(),
                nodes: 2
            },
        ]
    );
}

#[test]
fn a_satisfied_group_does_nothing() {
    let actions = plan(&[group("t-a", 1, 1, 0)], &[node("n1", Some("t-a"))]);
    assert!(actions.is_empty(), "{actions:?}");
}

#[test]
fn a_released_group_drains_its_nodes_and_never_grows() {
    let actions = plan(&[group("t-a", 1, 0, 0)], &[node("n1", Some("t-a"))]);
    assert_eq!(actions, vec![Action::Drain { node: "n1".into() }]);
}

#[test]
fn a_leaving_node_does_not_count_and_is_not_touched() {
    let mut leaving = node("n1", Some("t-a"));
    leaving.leaving = true;
    let actions = plan(&[group("t-a", 1, 0, 0)], &[leaving]);
    assert!(actions.is_empty(), "{actions:?}");
}

#[test]
fn excess_unregistered_free_capacity_is_terminated_directly() {
    let mut extra = node("n2", None);
    extra.registered = false;
    let actions = plan(&[group("t-a", 1, 1, 0)], &[node("n1", Some("t-a")), extra]);
    assert_eq!(
        actions,
        vec![Action::Terminate {
            node: "n2".into(),
            instance_id: "i-n2".into()
        }]
    );
}

#[test]
fn another_tenants_node_is_never_claimed() {
    let actions = plan(
        &[group("t-a", 1, 1, 0), group("t-b", 2, 1, 1)],
        &[node("n1", Some("t-b"))],
    );
    assert_eq!(
        actions,
        vec![Action::Grow {
            pool: "workers-12".into(),
            nodes: 2
        }]
    );
}

#[test]
fn an_encrypted_group_reserves_a_free_node_instead_of_claiming_it() {
    let actions = plan(&[encrypted_group("t-a", 1, 1)], &[node("n1", None)]);
    assert_eq!(
        actions,
        vec![Action::Reserve {
            node: "n1".into(),
            group: "t-a".into(),
            boot_id: "boot-1".into()
        }]
    );
}

#[test]
fn a_reserved_node_is_rekeyed_once_and_counts_toward_its_group() {
    let actions = plan(&[encrypted_group("t-a", 1, 1)], &[reserved("n1", "t-a")]);
    assert_eq!(
        actions,
        vec![Action::Rekey {
            node: "n1".into(),
            instance_id: "i-n1".into(),
            key_arn: KEY.into()
        }]
    );
}

#[test]
fn a_rekeyed_node_waits_while_it_is_on_the_old_boot_or_not_ready() {
    let mut same_boot = reserved("n1", "t-a");
    same_boot.encryption.rekeyed = true;
    same_boot.encryption.encrypted_boot = Some("boot-1".into());
    same_boot.encryption.encrypted_key = Some(KEY.into());
    let actions = plan(&[encrypted_group("t-a", 1, 1)], &[same_boot.clone()]);
    assert!(actions.is_empty(), "same boot: {actions:?}");

    let mut not_ready = same_boot;
    not_ready.encryption.boot_id = Some("boot-2".into());
    not_ready.encryption.encrypted_boot = Some("boot-2".into());
    not_ready.encryption.ready = false;
    let actions = plan(&[encrypted_group("t-a", 1, 1)], &[not_ready]);
    assert!(actions.is_empty(), "not ready: {actions:?}");
}

#[test]
fn a_rekeyed_node_is_claimed_only_when_its_new_boot_reports_the_groups_key() {
    let mut rebooted = reserved("n1", "t-a");
    rebooted.encryption.rekeyed = true;
    rebooted.encryption.boot_id = Some("boot-2".into());

    // No report yet, a stale report, or another key: wait.
    for (boot, key) in [
        (None, None),
        (Some("boot-1"), Some(KEY)),
        (Some("boot-2"), Some("arn:aws:kms:us-east-1:111122223333:key/other")),
    ] {
        let mut node = rebooted.clone();
        node.encryption.encrypted_boot = boot.map(str::to_string);
        node.encryption.encrypted_key = key.map(str::to_string);
        let actions = plan(&[encrypted_group("t-a", 1, 1)], &[node]);
        assert!(actions.is_empty(), "{boot:?} {key:?}: {actions:?}");
    }

    rebooted.encryption.encrypted_boot = Some("boot-2".into());
    rebooted.encryption.encrypted_key = Some(KEY.into());
    let actions = plan(&[encrypted_group("t-a", 1, 1)], &[rebooted]);
    assert_eq!(
        actions,
        vec![Action::Claim {
            node: "n1".into(),
            group: "t-a".into(),
            org_id: org(1)
        }]
    );
}

#[test]
fn a_node_reserved_for_one_group_is_never_taken_by_another() {
    let actions = plan(
        &[group("t-b", 2, 1, 0), encrypted_group("t-a", 1, 1)],
        &[reserved("n1", "t-a")],
    );
    assert_eq!(
        actions,
        vec![
            Action::Rekey {
                node: "n1".into(),
                instance_id: "i-n1".into(),
                key_arn: KEY.into()
            },
            Action::Grow {
                pool: "workers-12".into(),
                nodes: 2
            },
        ]
    );
}

#[test]
fn a_released_encrypted_group_drains_its_reserved_node() {
    let actions = plan(&[encrypted_group("t-a", 1, 0)], &[reserved("n1", "t-a")]);
    assert_eq!(actions, vec![Action::Drain { node: "n1".into() }]);
}
