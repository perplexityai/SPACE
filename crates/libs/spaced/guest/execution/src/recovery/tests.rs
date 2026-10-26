use super::*;
use pplx_space_spaced_guest_protocol::proto::operations::{PtyConfig, RecoveryStart};

#[test]
fn binding_covers_supplied_launch_values_but_not_environment_map_order() {
    let request = StartRequest {
        idempotency_key: Some("bound".into()),
        recovery: Some(RecoveryStart {
            contract_version: 1,
            expected_instance_id: ProcessRecoveryToken::new().to_string(),
            expected_activation_id: ProcessRecoveryToken::new().to_string(),
            command: vec!["/bin/true".into()],
        }),
        env: [("A".into(), "1".into()), ("B".into(), "2".into())].into(),
        ..Default::default()
    };
    let original = binding(&request);
    let mut reordered = request.clone();
    reordered.env = [("B".into(), "2".into()), ("A".into(), "1".into())].into();
    assert_eq!(binding(&reordered), original);
    let variations: &[fn(&mut StartRequest)] = &[
        |r| r.cwd = Some("/".into()),
        |r| {
            r.env.insert("A".into(), "changed".into());
        },
        |r| r.user = Some("nobody".into()),
        |r| {
            r.timeout = Some(prost_types::Duration {
                seconds: 1,
                nanos: 0,
            })
        },
        |r| r.background = true,
        |r| r.tag = Some("tag".into()),
        |r| r.stdin = true,
        |r| r.pty = Some(PtyConfig { size: None }),
        |r| r.clean_env = true,
        |r| r.process_id = Some("assigned".into()),
        |r| r.recovery.as_mut().unwrap().command.push("argument".into()),
    ];
    for change in variations {
        let mut other = request.clone();
        change(&mut other);
        assert_ne!(binding(&other), original, "changed supplied launch value");
    }
}
