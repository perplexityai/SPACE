use super::*;

fn populated_request() -> spaced_ops::StartRequest {
    spaced_ops::StartRequest {
        command: vec!["/bin/sh".into(), "-c".into(), "true".into()],
        cwd: Some("/workspace".into()),
        env: [("KEY".to_string(), "value".to_string())]
            .into_iter()
            .collect(),
        user: Some("space".into()),
        timeout: Some(prost_types::Duration {
            seconds: 5,
            nanos: 0,
        }),
        background: true,
        idempotency_key: Some("idem-1".into()),
        tag: Some("tag-1".into()),
        stdin: true,
        pty: Some(spaced_ops::PtyConfig {
            size: Some(spaced_ops::PtySize { rows: 24, cols: 80 }),
        }),
        clean_env: true,
        process_id: Some("assigned-proc".into()),
    }
}

// The host client rebuilds a StartRequest from these options, so anything this
// translation drops never reaches the guest. Assert every field a populated
// request carries, not just the one a change happens to touch.
#[test]
fn start_process_options_carries_every_request_field() {
    let request = populated_request();
    let options = start_process_options(&request).expect("options built");

    assert_eq!(options.command.argv(), request.command.as_slice());
    assert_eq!(
        options.cwd.as_ref().map(|cwd| cwd.expose().to_string()),
        request.cwd
    );
    assert_eq!(
        std::collections::BTreeMap::from(options.env.clone()),
        request.env.clone().into_iter().collect()
    );
    assert_eq!(
        options.user.as_ref().map(|user| user.as_str().to_string()),
        request.user
    );
    assert_eq!(options.timeout, Some(std::time::Duration::from_secs(5)));
    assert_eq!(options.background, request.background);
    assert_eq!(
        options
            .idempotency_key
            .as_ref()
            .map(|key| key.as_str().to_string()),
        request.idempotency_key
    );
    assert_eq!(options.tag, request.tag);
    assert_eq!(options.stdin, request.stdin);
    assert_eq!(options.pty, request.pty);
    assert_eq!(options.clean_env, request.clean_env);
    assert_eq!(
        options
            .process_id
            .as_ref()
            .map(|process_id| process_id.as_str().to_string()),
        request.process_id
    );
}

#[test]
fn start_process_options_rejects_an_invalid_assigned_process_id() {
    let mut request = populated_request();
    request.process_id = Some("bad\0id".into());

    let error = start_process_options(&request).expect_err("invalid process id rejected");
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
}
