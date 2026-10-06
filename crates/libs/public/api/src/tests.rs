use super::*;

#[cfg(feature = "schema")]
fn schema_pattern(schema: schemars::Schema) -> regex::Regex {
    let schema = serde_json::to_value(schema).expect("schema should serialize");
    let pattern = schema["pattern"]
        .as_str()
        .expect("id schema should contain a pattern");
    regex::Regex::new(pattern).expect("id schema pattern should compile")
}

#[cfg(feature = "schema")]
#[test]
fn generated_id_patterns_follow_the_typed_id_contract() {
    let mut generator = schemars::generate::SchemaSettings::draft2020_12().into_generator();
    let sandbox_pattern = schema_pattern(schema::public_sandbox_id(&mut generator));
    assert!(sandbox_pattern.is_match(&PublicSandboxId::new().to_string()));
    assert!(!sandbox_pattern.is_match(&format!("{}{}", PublicSandboxId::PREFIX, "0".repeat(36))));

    let session_pattern = schema_pattern(schema::data_plane_session_id(&mut generator));
    for kind in SessionKind::ALL {
        assert!(session_pattern.is_match(&DataPlaneSessionId::new_for_kind(*kind).to_string()));
    }

    let process_pattern = schema_pattern(schema::process_session_id(&mut generator));
    assert!(process_pattern.is_match(&ProcessSessionId::new().to_string()));
}

#[cfg(feature = "serde")]
#[test]
fn a_process_request_without_a_user_serializes_to_its_pre_user_shape() {
    // This value becomes the session's `adapter_state_json`, which the
    // idempotency fingerprint hashes. A new `"user": null` key would change
    // the fingerprint of an unchanged request, so a retry spanning the
    // deploy would conflict rather than replay.
    let request = CreateProcessRequest {
        command: vec!["true".to_string()],
        cwd: None,
        env: Default::default(),
        pty: None,
        stdin: None,
        user: None,
        timeout: None,
    };

    let value = serde_json::to_value(&request).expect("request should serialize");

    assert_eq!(value.get("user"), None);
    assert_eq!(
        serde_json::from_value::<CreateProcessRequest>(value).expect("round trip"),
        request
    );
}

#[cfg(feature = "serde")]
#[test]
fn a_process_request_with_a_user_carries_it() {
    let request = CreateProcessRequest {
        command: vec!["true".to_string()],
        cwd: None,
        env: Default::default(),
        pty: None,
        stdin: None,
        user: Some("root".to_string()),
        timeout: None,
    };

    let value = serde_json::to_value(&request).expect("request should serialize");

    assert_eq!(value["user"], "root");
    assert_eq!(
        serde_json::from_value::<CreateProcessRequest>(value).expect("round trip"),
        request
    );
}

#[cfg(feature = "serde")]
#[test]
fn process_and_command_requests_share_timeout_validation() {
    for (timeout, valid) in [
        ("1.5m", true),
        (" 1s", false),
        ("1000000000s", false),
        ("0.0000000001s", false),
    ] {
        let value = serde_json::json!({ "command": ["true"], "timeout": timeout });
        let command_valid = serde_json::from_value::<CreateCommandRequest>(value.clone()).is_ok();
        let process_valid = serde_json::from_value::<CreateProcessRequest>(value).is_ok();

        assert_eq!(command_valid, valid, "command timeout {timeout}");
        assert_eq!(process_valid, valid, "process timeout {timeout}");
    }
}

#[test]
fn public_id_namespaces_round_trip_and_reject_wrong_prefixes() {
    let domain = SandboxId::new();
    let public = PublicSandboxId::from(domain);
    assert_eq!(public.as_uuid(), domain.as_uuid());
    assert_eq!(SandboxId::from(public), domain);

    let encoded = public.to_string();
    assert!(encoded.starts_with(PUBLIC_SANDBOX_ID_PREFIX));
    assert_eq!(encoded.parse::<PublicSandboxId>().unwrap(), public);

    let op = OperationId::new().to_string();
    op.parse::<PublicSandboxId>().unwrap_err();

    for (session, kind) in [
        (
            DataPlaneSessionId::Terminal(TerminalSessionId::new()),
            SessionKind::Terminal,
        ),
        (
            DataPlaneSessionId::Process(ProcessSessionId::new()),
            SessionKind::Process,
        ),
        (
            DataPlaneSessionId::FileWatch(FileWatchSessionId::new()),
            SessionKind::FileWatch,
        ),
        (
            DataPlaneSessionId::Port(PortSessionId::new()),
            SessionKind::Port,
        ),
        (
            DataPlaneSessionId::Protocol(ProtocolSessionId::new()),
            SessionKind::Protocol,
        ),
    ] {
        let parsed = session.to_string().parse::<DataPlaneSessionId>().unwrap();
        assert_eq!(parsed, session);
        assert_eq!(parsed.kind(), kind);
        assert_eq!(parsed.as_uuid(), session.as_uuid());
    }
}

#[test]
fn public_wire_enums_use_canonical_strings() {
    assert_eq!(
        "create_sandbox".parse::<OperationKind>().unwrap(),
        OperationKind::CreateSandbox
    );
    assert_eq!(
        "update_sandbox".parse::<OperationKind>().unwrap(),
        OperationKind::UpdateSandbox
    );
    assert_eq!(
        "fork_sandbox".parse::<OperationKind>().unwrap(),
        OperationKind::ForkSandbox
    );
    assert_eq!(
        SandboxLifecycleAction::Checkpoint.operation_kind(),
        OperationKind::CheckpointSandbox
    );
    assert_eq!(
        "rollback".parse::<SandboxLifecycleAction>().unwrap(),
        SandboxLifecycleAction::Rollback
    );
    assert_eq!(
        DataPlaneScope::TerminalConnect.to_string(),
        "terminal:connect"
    );
    assert_eq!(
        DataPlaneErrorCode::BackendStreamError.to_string(),
        "backend_stream_error"
    );
    assert_eq!(
        DataPlaneResyncReason::CursorOutOfWindow.to_string(),
        "cursor_out_of_window"
    );
    assert_eq!(
        SandboxFailureCode::RuntimeFailure.to_string(),
        "runtime_failure"
    );
    for action in SandboxIdleAction::ALL {
        assert_eq!(
            action.as_str().parse::<SandboxIdleAction>().unwrap(),
            *action
        );
    }
    "terminal_connect".parse::<DataPlaneScope>().unwrap_err();

    #[cfg(feature = "serde")]
    {
        let future_action: SandboxIdleAction =
            serde_json::from_value(serde_json::json!("hibernate")).unwrap();
        assert!(!future_action.is_known());
        assert_eq!(
            serde_json::to_value(future_action).unwrap(),
            serde_json::json!("hibernate")
        );
        assert_eq!(
            serde_json::to_value(OperationState::Queued).unwrap(),
            serde_json::json!("queued")
        );
        assert_eq!(
            serde_json::from_value::<PublicErrorCode>(serde_json::json!("not_found")).unwrap(),
            PublicErrorCode::NotFound
        );

        let session = DataPlaneSessionId::Protocol(ProtocolSessionId::new());
        let encoded = serde_json::to_value(session).unwrap();
        assert_eq!(
            serde_json::from_value::<DataPlaneSessionId>(encoded).unwrap(),
            session
        );
        assert_eq!(
            serde_json::to_value(SessionTargetProtocol::Cdp).unwrap(),
            serde_json::json!("cdp")
        );
        assert_eq!(
            serde_json::from_value::<SessionTargetProtocol>(serde_json::json!("http")).unwrap(),
            SessionTargetProtocol::Http
        );
    }
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_idle_timeout_wire_parsing_matches_whole_second_stage_limits() {
    for (timeout, expected_seconds) in [
        (serde_json::Value::Null, Some(None)),
        (serde_json::json!("600s"), Some(Some(600))),
        (serde_json::json!("1.5m"), Some(Some(90))),
        (serde_json::json!("0.5h"), Some(Some(1800))),
        (serde_json::json!("1d"), Some(Some(86400))),
        (serde_json::json!(" 600s "), Some(Some(600))),
        (
            serde_json::json!("9223372036854775807s"),
            Some(Some(i64::MAX as u64)),
        ),
        (
            serde_json::json!("153722867280912930m"),
            Some(Some(9_223_372_036_854_775_800)),
        ),
        (serde_json::json!("0s"), None),
        (serde_json::json!("0.0m"), None),
        (serde_json::json!("1.5s"), None),
        (serde_json::json!("0.001m"), None),
        (serde_json::json!("1.0000000001s"), None),
        (serde_json::json!("9223372036854775808s"), None),
        (serde_json::json!("153722867280912931m"), None),
        (serde_json::json!("18446744073709551616s"), None),
        (serde_json::json!("-1s"), None),
        (serde_json::json!("500ms"), None),
        (serde_json::json!(""), None),
        (serde_json::json!(600), None),
        (serde_json::json!(false), None),
    ] {
        let request = serde_json::json!({ "template": "seed", "idle_timeout": timeout });
        let parsed = serde_json::from_value::<CreateSandboxRequest>(request.clone());
        match expected_seconds {
            Some(seconds) => assert_eq!(
                parsed.unwrap().idle_timeout_secs().unwrap(),
                seconds,
                "{request}"
            ),
            None => assert!(parsed.is_err(), "{request}"),
        }
    }
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_idle_action_is_closed_on_input() {
    for (action, accepted) in [
        (serde_json::Value::Null, true),
        (serde_json::json!("delete"), true),
        (serde_json::json!("suspend"), true),
        (serde_json::json!("pause"), false),
        (serde_json::json!("stop"), false),
        (serde_json::json!("hibernate"), false),
        (serde_json::json!(12), false),
        (serde_json::json!(false), false),
    ] {
        let request = serde_json::json!({ "template": "seed", "idle_timeout": "600s", "idle_action": action });
        assert_eq!(
            serde_json::from_value::<CreateSandboxRequest>(request.clone()).is_ok(),
            accepted,
            "{request}"
        );
    }
}

#[test]
fn sandbox_lifecycle_state_uses_wire_value_identity() {
    assert_eq!(
        "running".parse::<SandboxLifecycleState>().unwrap(),
        SandboxLifecycleState::Running
    );

    let future_state = "hibernating".parse::<SandboxLifecycleState>().unwrap();
    assert_eq!(future_state.as_str(), "hibernating");
    assert_eq!(future_state.to_string(), "hibernating");
    assert_eq!(future_state.as_ref(), "hibernating");

    // The public catch-all can be constructed directly, so equality, ordering,
    // and hashing follow the canonical wire value rather than enum layout.
    let known_alias = SandboxLifecycleState::Other("running".to_string());
    assert_eq!(known_alias, SandboxLifecycleState::Running);
    assert_eq!(
        known_alias.cmp(&SandboxLifecycleState::Running),
        std::cmp::Ordering::Equal
    );
    // Known states retain their pre-open-enum declaration ordering.
    assert!(SandboxLifecycleState::Stopped < SandboxLifecycleState::Starting);
    let states = std::collections::HashSet::from([known_alias, SandboxLifecycleState::Running]);
    assert_eq!(states.len(), 1);
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_lifecycle_state_serde_preserves_strings_and_rejects_other_json_types() {
    for wire_value in ["running", "hibernating", "", "future.state/v2", "休止中"] {
        let encoded = serde_json::json!(wire_value);
        let state = serde_json::from_value::<SandboxLifecycleState>(encoded.clone()).unwrap();
        assert_eq!(state.as_str(), wire_value);
        assert_eq!(serde_json::to_value(state).unwrap(), encoded);
    }

    for malformed in [
        serde_json::Value::Null,
        serde_json::json!(false),
        serde_json::json!(42),
        serde_json::json!([]),
        serde_json::json!({}),
    ] {
        serde_json::from_value::<SandboxLifecycleState>(malformed).unwrap_err();
    }
}

#[cfg(feature = "serde")]
#[test]
fn public_sandbox_failure_code_has_an_exact_nullable_wire_shape() {
    let sandbox_id = PublicSandboxId::from_uuid(
        Uuid::parse_str("019d0000-0000-7000-8000-000000000001").unwrap(),
    );
    let timestamp = time::OffsetDateTime::from_unix_timestamp(0).unwrap();
    let mut sandbox = PublicSandbox {
        id: sandbox_id,
        name: None,
        state: SandboxLifecycleState::Failed,
        failure_code: Some(SandboxFailureCode::RuntimeFailure),
        conditions: Vec::new(),
        template: None,
        template_digest: None,
        resources: SandboxResources {
            vcpu: None,
            memory_mib: None,
            disk_mib: None,
        },
        metadata: PublicSandboxMetadata::new(),
        expires_at: None,
        idle_timeout: None,
        idle_action: None,
        created_at: timestamp,
        updated_at: timestamp,
    };

    assert_eq!(
        serde_json::to_value(&sandbox).unwrap(),
        serde_json::json!({
            "id": "sbx_019d0000-0000-7000-8000-000000000001",
            "name": null,
            "state": "failed",
            "failure_code": "runtime_failure",
            "conditions": [],
            "template": null,
            "template_digest": null,
            "resources": {
                "vcpu": null,
                "memory_mib": null,
                "disk_mib": null
            },
            "metadata": {},
            "expires_at": null,
            "idle_timeout": null,
            "idle_action": null,
            "created_at": "1970-01-01T00:00:00Z",
            "updated_at": "1970-01-01T00:00:00Z"
        })
    );

    sandbox.failure_code = None;
    let mut serialized = serde_json::to_value(&sandbox).unwrap();
    assert_eq!(serialized["failure_code"], serde_json::Value::Null);
    assert!(serialized.get("failure_reason").is_none());
    for field in ["expires_at", "idle_timeout", "idle_action"] {
        serialized.as_object_mut().unwrap().remove(field);
    }
    assert_eq!(
        serde_json::from_value::<PublicSandbox>(serialized).unwrap(),
        sandbox
    );
}

#[cfg(feature = "serde")]
#[test]
fn create_sandbox_request_uses_public_contract_fields() {
    let decoded: CreateSandboxRequest = serde_json::from_value(serde_json::json!({
        "template": "seed:latest",
        "runtime": "firecracker",
        "name": "demo",
        "env": {"FOO": "bar"},
        "metadata": {"purpose": "test", "attempt": 1, "interactive": true},
        "resources": {"vcpu": 2, "memory_mib": 1024, "disk_mib": 4096},
        "network_policy": {
            "version": "v1",
            "egress": {"default_action": "allow"}
        }
    }))
    .unwrap();

    assert_eq!(decoded.template.to_string(), "seed:latest");
    assert_eq!(decoded.runtime, Some(RuntimeKind::Firecracker));
    assert_eq!(
        decoded.metadata.get("purpose"),
        Some(&PublicSandboxMetadataValue::String("test".to_string()))
    );
    assert_eq!(
        decoded
            .network_policy
            .as_ref()
            .map(NetworkPolicyV1::default_action),
        Some(pplx_space_net_common::EgressDefaultAction::Allow)
    );
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_mutation_requests_reject_v2_and_accept_domain_denies() {
    let v1_create = serde_json::json!({
        "template": "seed",
        "network_policy": {
            "version": "v1",
            "egress": {"default_action": "deny"}
        }
    });
    serde_json::from_value::<CreateSandboxRequest>(v1_create.clone()).unwrap();
    let mut v2_create = v1_create;
    v2_create["network_policy"] = serde_json::json!({
        "version": "v2",
        "egress": {
            "deny": [{"domain": "evil.example"}],
            "allow": [{"cidr": "0.0.0.0/0"}]
        }
    });
    serde_json::from_value::<CreateSandboxRequest>(v2_create).unwrap_err();

    let domain_deny_create = serde_json::json!({
        "template": "seed",
        "network_policy": {
            "version": "v1",
            "egress": {
                "default_action": "deny",
                "deny": [{"domain": "blocked.example.com"}]
            }
        }
    });
    serde_json::from_value::<CreateSandboxRequest>(domain_deny_create).unwrap();

    let v1_update = serde_json::json!({
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {"default_action": "deny"}
            }
        }
    });
    serde_json::from_value::<UpdateSandboxRequest>(v1_update.clone()).unwrap();
    let mut v2_update = v1_update;
    v2_update["network_policy"]["policy"] = serde_json::json!({
        "version": "v2",
        "egress": {
            "deny": [{"domain": "evil.example"}],
            "allow": [{"cidr": "0.0.0.0/0"}]
        }
    });
    serde_json::from_value::<UpdateSandboxRequest>(v2_update).unwrap_err();

    let domain_deny_update = serde_json::json!({
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {
                    "default_action": "deny",
                    "deny": [{"domain": "blocked.example.com"}]
                }
            }
        }
    });
    serde_json::from_value::<UpdateSandboxRequest>(domain_deny_update).unwrap();
}

#[cfg(feature = "serde")]
#[test]
fn update_sandbox_request_uses_public_contract_fields() {
    let decoded: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({
        "metadata": {"purpose": "resize", "attempt": 2},
        "resources": {"vcpu": 4, "memory_mib": 2048},
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {"default_action": "deny"}
            }
        }
    }))
    .unwrap();

    assert_eq!(decoded.resources.as_ref().and_then(|r| r.vcpu), Some(4));
    let Some(SandboxNetworkPolicyPatch::Set { policy }) = decoded.network_policy.as_ref() else {
        panic!("expected a V1 set network-policy patch");
    };
    assert_eq!(
        policy.default_action(),
        pplx_space_net_common::EgressDefaultAction::Deny
    );
    assert_eq!(
        decoded.metadata.as_ref().and_then(|m| m.get("purpose")),
        Some(&PublicSandboxMetadataValue::String("resize".to_string()))
    );
}

#[cfg(feature = "serde")]
#[test]
fn full_document_update_round_trips_tls_interception_on() {
    let wire = serde_json::json!({
        "metadata": null,
        "resources": null,
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {"default_action": "deny"},
                "tls_interception": "on"
            }
        },
        "auto_lifecycle": null
    });
    let decoded: UpdateSandboxRequest = serde_json::from_value(wire).unwrap();
    let Some(SandboxNetworkPolicyPatch::Set { policy }) = decoded.network_policy.as_ref() else {
        panic!("expected a set policy");
    };
    assert_eq!(
        policy.tls_interception,
        pplx_space_net_common::TlsInterception::On
    );

    let round_tripped: UpdateSandboxRequest =
        serde_json::from_value(serde_json::to_value(&decoded).unwrap()).unwrap();
    assert_eq!(round_tripped, decoded);
}

#[cfg(feature = "serde")]
#[test]
fn sandbox_update_accepts_reference_only_network_credentials_with_optional_port_and_path() {
    let decoded: UpdateSandboxRequest = serde_json::from_value(serde_json::json!({
        "network_policy": {
            "mode": "set",
            "policy": {
                "version": "v1",
                "egress": {
                    "default_action": "deny",
                    "allow": [{"domain": "api.example.com"}]
                },
                "tls_interception": "on",
                "network_credential_rules": [{
                    "rule_id": "example",
                    "destination": {
                        "exact_host": "API.EXAMPLE.COM.",
                        "path_prefix": "/v1"
                    },
                    "credential_header_bindings": [{
                        "header_name": "Authorization",
                        "credential_reference_id": "018f1e9d-7b7a-7d3a-8000-000000000001"
                    }],
                    "supported_http_methods": ["GET", "POST"]
                }]
            }
        }
    }))
    .unwrap();
    let Some(SandboxNetworkPolicyPatch::Set { policy }) = decoded.network_policy else {
        panic!("expected a set policy");
    };
    let rule = &policy.network_credential_rules[0];
    assert_eq!(rule.destination.exact_host.as_str(), "api.example.com");
    assert_eq!(rule.destination.port, 443);
    assert_eq!(rule.destination.path_prefix.as_deref(), Some("/v1"));
    assert_eq!(
        rule.credential_header_bindings[0].header_name,
        "authorization"
    );

    let encoded = serde_json::to_value(policy).unwrap();
    assert!(
        encoded["network_credential_rules"][0]["destination"]
            .get("port")
            .is_none(),
        "default port must remain omitted from canonical public JSON"
    );
}

#[cfg(feature = "serde")]
#[test]
fn file_read_request_requires_bounded_length() {
    serde_json::from_value::<FileReadRequest>(serde_json::json!({
        "path": "/workspace/data.txt",
        "offset": 0,
        "user": null
    }))
    .unwrap_err();

    let decoded: FileReadRequest = serde_json::from_value(serde_json::json!({
        "path": "/workspace/data.txt",
        "offset": 4,
        "length": 64,
        "user": null
    }))
    .unwrap();
    assert_eq!(decoded.length, 64);
}

#[cfg(feature = "serde")]
#[test]
fn public_response_types_ignore_additive_fields() {
    let decoded: FileWriteResponse = serde_json::from_value(serde_json::json!({
        "bytes_written": 12,
        "checksum": "future-field"
    }))
    .unwrap();

    assert_eq!(decoded.bytes_written, 12);
}

#[cfg(feature = "serde")]
#[test]
fn data_plane_server_frame_accepts_unknown_future_type() {
    let decoded: DataPlaneServerFrame = serde_json::from_value(serde_json::json!({
        "type": "future_event",
        "payload": {"ok": true}
    }))
    .unwrap();

    assert_eq!(decoded, DataPlaneServerFrame::Unknown);
}

#[cfg(feature = "serde")]
#[test]
fn data_plane_control_frames_round_trip() {
    let signal: DataPlaneClientFrame = serde_json::from_value(serde_json::json!({
        "type": "signal",
        "seq": 11,
        "signal": 15
    }))
    .unwrap();
    assert_eq!(
        signal,
        DataPlaneClientFrame::Signal {
            seq: 11,
            signal: 15
        }
    );

    let close_stdin: DataPlaneClientFrame = serde_json::from_value(serde_json::json!({
        "type": "close_stdin",
        "seq": 12
    }))
    .unwrap();
    assert_eq!(close_stdin, DataPlaneClientFrame::CloseStdin { seq: 12 });

    let heartbeat: DataPlaneClientFrame = serde_json::from_value(serde_json::json!({
        "type": "heartbeat",
        "seq": 13
    }))
    .unwrap();
    assert_eq!(heartbeat, DataPlaneClientFrame::Heartbeat { seq: 13 });

    let accepted: DataPlaneServerFrame = serde_json::from_value(serde_json::json!({
        "type": "control_accepted",
        "seq": 13
    }))
    .unwrap();
    assert_eq!(accepted, DataPlaneServerFrame::ControlAccepted { seq: 13 });
}

#[test]
fn page_cursor_is_bounded() {
    assert_eq!(PageCursor::parse("abc").unwrap().as_str(), "abc");
    PageCursor::parse("").unwrap_err();
    PageCursor::parse("x".repeat(MAX_PAGE_CURSOR_BYTES + 1)).unwrap_err();
    PageCursor::parse("abc\0def").unwrap_err();
}

#[test]
fn request_fingerprint_is_canonical_sha256_hex() {
    let uppercase = "A".repeat(REQUEST_FINGERPRINT_HEX_LEN);
    let fingerprint = RequestFingerprint::parse(uppercase).unwrap();
    assert_eq!(
        fingerprint.as_str(),
        "a".repeat(REQUEST_FINGERPRINT_HEX_LEN)
    );

    RequestFingerprint::parse("a".repeat(63)).unwrap_err();
    RequestFingerprint::parse("g".repeat(REQUEST_FINGERPRINT_HEX_LEN)).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn operation_result_uses_tagged_public_contract() {
    let sandbox_id = PublicSandboxId::new();
    let result = PublicOperationResult::Sandbox(PublicSandboxOperationResult {
        sandbox_id,
        state: SandboxLifecycleState::Running,
        action: Some(SandboxLifecycleAction::Wake),
        controller_response: None,
    });

    let encoded = serde_json::to_value(&result).unwrap();
    assert_eq!(
        encoded,
        serde_json::json!({
            "type": "sandbox",
            "sandbox_id": sandbox_id.to_string(),
            "state": "running",
            "action": "wake",
            "controller_response": null
        })
    );
    assert_eq!(
        serde_json::from_value::<PublicOperationResult>(encoded).unwrap(),
        result
    );

    let fork = PublicOperationResult::Fork(PublicForkOperationResult {
        results: vec![serde_json::json!({ "sandboxID": sandbox_id.to_string() })],
    });
    let encoded = serde_json::to_value(&fork).unwrap();
    assert_eq!(
        encoded,
        serde_json::json!({
            "type": "fork",
            "results": [{ "sandboxID": sandbox_id.to_string() }]
        })
    );
    assert_eq!(
        serde_json::from_value::<PublicOperationResult>(encoded).unwrap(),
        fork
    );
}

#[cfg(feature = "serde")]
#[test]
fn set_timeout_uses_whole_second_duration_parsing_and_uint32_limit() {
    for (timeout, expected) in [
        ("1s", Some(1)),
        ("1.5m", Some(90)),
        ("0.5h", Some(1800)),
        ("4294967295s", Some(u32::MAX)),
        ("4294967296s", None),
        ("0s", None),
        ("1.5s", None),
        ("nope", None),
    ] {
        let request = serde_json::from_value::<SetSandboxTimeoutRequest>(
            serde_json::json!({"timeout": timeout}),
        );
        assert_eq!(
            request.ok().and_then(|request| request.timeout_secs().ok()),
            expected,
            "{timeout}"
        );
    }
    for value in [
        serde_json::json!({}),
        serde_json::json!({"timeout": null}),
        serde_json::json!({"timeout": 60}),
    ] {
        serde_json::from_value::<SetSandboxTimeoutRequest>(value).unwrap_err();
    }
}
