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
    }
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
