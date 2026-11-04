use pplx_space_common_core::host::linux::identities::{user_names, users};

use super::*;

const SBXMGR: &str = user_names::SPACE_SBXMGR;
const TMPLMGR: &str = user_names::SPACE_TMPLMGR;

fn authorizer() -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new(
        "test-service",
        [
            UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR),
            UdsPeerAccount::with_linux_user(users::SPACE_TMPLMGR),
        ],
    )
}

#[test]
fn classifies_root() {
    let auth = authorizer();
    assert_eq!(
        auth.classify(&PeerIdentity::new(0, 0, Some(1))).principal,
        UdsPeerPrincipal::Root
    );
}

#[test]
fn classifies_known_fallback_uid() {
    let auth = authorizer();
    assert_eq!(
        auth.classify(&PeerIdentity::new(
            users::SPACE_SBXMGR.uid,
            users::SPACE_SBXMGR.primary_group.gid,
            Some(42),
        ))
        .principal,
        UdsPeerPrincipal::Service(SBXMGR)
    );
}

#[test]
fn classifies_unknown_uid_as_other() {
    let auth = authorizer();
    assert_eq!(
        auth.classify(&PeerIdentity::new(12_345, 12_345, Some(42)))
            .principal,
        UdsPeerPrincipal::Other
    );
}

#[test]
fn authorizes_allowed_principal() {
    let auth = authorizer();
    let peer = auth
        .authorize_identity(
            PeerIdentity::new(
                users::SPACE_TMPLMGR.uid,
                users::SPACE_TMPLMGR.primary_group.gid,
                Some(42),
            ),
            "test_op",
            &[UdsPeerPrincipal::Root, UdsPeerPrincipal::Service(TMPLMGR)],
        )
        .unwrap();
    assert_eq!(peer.principal(), UdsPeerPrincipal::Service(TMPLMGR));
}

#[test]
fn rejects_disallowed_principal() {
    let auth = authorizer();
    let err = auth
        .authorize_identity(
            PeerIdentity::new(
                users::SPACE_SBXMGR.uid,
                users::SPACE_SBXMGR.primary_group.gid,
                Some(42),
            ),
            "test_op",
            &[UdsPeerPrincipal::Service(TMPLMGR)],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[test]
fn classifies_operator_role_account() {
    let auth = UdsPeerAuthorizer::new(
        "test-service",
        [UdsPeerAccount::role_with_fallback_uid(
            "deploy",
            LocalRole::Operator,
            2001,
        )],
    );

    assert_eq!(
        auth.classify(&PeerIdentity::new(2001, 2001, Some(42)))
            .principal,
        UdsPeerPrincipal::Role(LocalRole::Operator)
    );
}

#[test]
fn higher_roles_satisfy_lower_role_requirements() {
    let auth = UdsPeerAuthorizer::new(
        "test-service",
        [UdsPeerAccount::role_with_fallback_uid(
            "deploy",
            LocalRole::Admin,
            2001,
        )],
    );

    let peer = auth
        .authorize_identity(
            PeerIdentity::new(2001, 2001, Some(42)),
            "test_op",
            &[UdsPeerPrincipal::Role(LocalRole::Operator)],
        )
        .unwrap();
    assert_eq!(peer.principal(), UdsPeerPrincipal::Role(LocalRole::Admin));
}

#[test]
fn lower_roles_do_not_satisfy_higher_role_requirements() {
    let auth = UdsPeerAuthorizer::new(
        "test-service",
        [UdsPeerAccount::role_with_fallback_uid(
            "dev",
            LocalRole::Observer,
            2000,
        )],
    );

    let err = auth
        .authorize_identity(
            PeerIdentity::new(2000, 2000, Some(42)),
            "test_op",
            &[UdsPeerPrincipal::Role(LocalRole::Operator)],
        )
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[test]
fn authorize_identity_records_peer_principal_on_current_span() {
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::{LookupSpan, Registry};

    // Layer that captures the latest value of `rpc.client.peer_principal`
    // recorded on any span via `Span::record(...)`.
    #[derive(Default, Clone)]
    struct CapturedPrincipal(Arc<Mutex<Option<String>>>);

    impl<S> tracing_subscriber::Layer<S> for CapturedPrincipal
    where
        S: tracing::Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_record(
            &self,
            _id: &tracing::span::Id,
            values: &tracing::span::Record<'_>,
            _ctx: Context<'_, S>,
        ) {
            struct Visit<'a>(&'a Arc<Mutex<Option<String>>>);
            impl tracing::field::Visit for Visit<'_> {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "rpc.client.peer_principal" {
                        *self.0.lock().unwrap() = Some(value.to_string());
                    }
                }
                fn record_debug(
                    &mut self,
                    _field: &tracing::field::Field,
                    _value: &dyn std::fmt::Debug,
                ) {
                }
            }
            values.record(&mut Visit(&self.0));
        }
    }

    let captured = CapturedPrincipal::default();
    let subscriber = Registry::default().with(captured.clone());

    tracing::subscriber::with_default(subscriber, || {
        // The grpc.server span declares this field as Empty so callers can
        // record it later — mirror that here.
        let span = tracing::info_span!(
            "grpc.server",
            "rpc.client.peer_principal" = tracing::field::Empty,
        );
        let _entered = span.enter();

        authorizer()
            .authorize_identity(
                PeerIdentity::new(
                    users::SPACE_SBXMGR.uid,
                    users::SPACE_SBXMGR.primary_group.gid,
                    Some(42),
                ),
                "test_op",
                &[UdsPeerPrincipal::Service(SBXMGR)],
            )
            .unwrap();
    });

    let recorded = captured.0.lock().unwrap().clone();
    assert_eq!(recorded.as_deref(), Some(SBXMGR));
}

// /proc/<pid>/status parser tests live next to the parser at
// `peer_auth/os/linux.rs` since the parser is Linux-only.

// --- cross-uid readability preflight ------------------------------------

fn resolved_account(uids: &[u32], checks: PeerProcessChecks) -> ResolvedUdsPeerAccount {
    ResolvedUdsPeerAccount {
        principal: UdsPeerPrincipal::Service("test-peer"),
        uids: uids.to_vec(),
        process_checks: checks,
    }
}

#[test]
fn preflight_warns_only_for_unprivileged_cross_uid_checks() {
    let armed = PeerProcessChecks {
        binary_paths: &["/opt/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let accounts = vec![resolved_account(&[1001], armed)];

    // Cross-uid without privilege is unreadable — the READ-mode DAC gate,
    // independent of any yama ptrace_scope value.
    assert!(cross_uid_checks_unreadable(&accounts, 1002, 0));
    // Same-uid peers are always readable.
    assert!(!cross_uid_checks_unreadable(&accounts, 1001, 0));
    // Effective CAP_SYS_PTRACE reads any peer; euid 0 implies it.
    assert!(!cross_uid_checks_unreadable(
        &accounts,
        1002,
        capability::CAP_SYS_PTRACE
    ));
    assert!(!cross_uid_checks_unreadable(&accounts, 0, 0));

    // Accounts without configured checks never warrant the warning.
    let unarmed = vec![resolved_account(&[1001], PeerProcessChecks::EMPTY)];
    assert!(!cross_uid_checks_unreadable(&unarmed, 1002, 0));

    // A multi-uid account is cross-uid as soon as ANY of its uids differs
    // from ours, even if one matches.
    let multi = vec![resolved_account(&[1001, 1002], armed)];
    assert!(cross_uid_checks_unreadable(&multi, 1001, 0));
}

// --- evaluate_process_checks decision logic ----------------------------

fn snapshot(exe: &str, uid: u32, cap_effective: u64) -> PeerProcessSnapshot {
    PeerProcessSnapshot {
        exe: PathBuf::from(exe),
        exe_deleted: false,
        effective_uid: uid,
        cap_effective,
    }
}

#[test]
fn evaluate_passes_when_unconfigured() {
    let checks = PeerProcessChecks::EMPTY;
    let snap = snapshot("/opt/space/bin/space-sbxmgr-svc", 1001, 0);
    evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new())
        .expect("EMPTY checks always pass");
}

#[test]
fn evaluate_rejects_uid_drift() {
    let checks = PeerProcessChecks::EMPTY;
    // Authorizer saw uid=1001 from SO_PEERCRED but /proc says
    // effective uid=4242 — pid was reused.
    let snap = snapshot("/opt/space/bin/space-sbxmgr-svc", 4242, 0);
    let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new()).unwrap_err();
    assert!(matches!(
        err,
        ProcessCheckFailure::UidDrift {
            expected: 1001,
            found: 4242
        }
    ));
}

#[test]
fn evaluate_accepts_allowed_binary() {
    let checks = PeerProcessChecks {
        binary_paths: &[
            "/opt/space/bin/space-sbxmgr-svc",
            "/space/bin/space-sbxmgr-svc",
        ],
        ..PeerProcessChecks::EMPTY
    };
    for path in checks.binary_paths {
        let snap = snapshot(path, 1001, 0);
        evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new()).unwrap_or_else(|e| {
            panic!("expected {path:?} to be allowed: {e}");
        });
    }
}

#[test]
fn evaluate_admits_the_content_addressed_systemd_install() {
    // The systemd deploy path uploads bytes to
    // `/space/apps/<name>-<sha256[:8]>` and points the `/space/bin/<name>`
    // symlink at it, so an exec through the stable link makes
    // `/proc/<pid>/exe` report the digest target. The stable link path in
    // the allowlist must admit that target, or every systemd peer is
    // denied under enforce.
    let checks = PeerProcessChecks {
        binary_paths: &["/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/space/apps/space-sbxmgr-svc-1a2b3c4d", 1001, 0);
    evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new())
        .expect("the deploy script's content-addressed install must satisfy its stable link entry");
}

#[test]
fn volmgr_exe_is_admitted_only_for_its_service_account() {
    use pplx_space_common_core::host::linux::identities::users;

    let volume_checks = PeerProcessChecks {
        binary_paths: pplx_space_common_core::host::linux::paths::SPACE_VOLMGR_SVC_BINARIES,
        ..PeerProcessChecks::EMPTY
    };
    let sandbox_checks = PeerProcessChecks {
        binary_paths: pplx_space_common_core::host::linux::paths::SPACE_SBXMGR_UID_BINARIES,
        ..PeerProcessChecks::EMPTY
    };
    for exe in [
        "/opt/space/bin/space-volmgr-svc",
        "/space/apps/space-volmgr-svc-1a2b3c4d",
    ] {
        let snap = snapshot(exe, users::SPACE_VOLMGR.uid, 0);
        evaluate_process_checks(&volume_checks, users::SPACE_VOLMGR.uid, &snap, |_| {
            Vec::new()
        })
        .unwrap();
        let snap = snapshot(exe, users::SPACE_SBXMGR.uid, 0);
        let error =
            evaluate_process_checks(&sandbox_checks, users::SPACE_SBXMGR.uid, &snap, |_| {
                Vec::new()
            })
            .unwrap_err();
        assert!(matches!(error, ProcessCheckFailure::BinaryNotAllowed(_)));
    }
    for exe in [
        "/opt/space/bin/space-sbxmgr-svc",
        "/space/apps/space-sbxmgr-svc-1a2b3c4d",
    ] {
        let snap = snapshot(exe, users::SPACE_SBXMGR.uid, 0);
        evaluate_process_checks(&sandbox_checks, users::SPACE_SBXMGR.uid, &snap, |_| {
            Vec::new()
        })
        .unwrap();
    }
}

#[test]
fn content_addressed_install_does_not_satisfy_the_helm_entry() {
    // Only /space/bin entries are deploy-script symlinks into /space/apps;
    // the Helm layout installs real files under /opt/space/bin and can never
    // legitimately produce the digest shape. The digest form must not widen
    // what the Helm entry admits.
    let checks = PeerProcessChecks {
        binary_paths: &["/opt/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/space/apps/space-sbxmgr-svc-1a2b3c4d", 1001, 0);
    let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new())
        .expect_err("a digest install must not satisfy an entry from the Helm layout");
    assert!(matches!(err, ProcessCheckFailure::BinaryNotAllowed(_)));
}

#[test]
fn evaluate_rejects_lookalike_content_addressed_installs() {
    // The digest form is exactly `<apps dir>/<entry basename>-<8 lowercase
    // hex>`; anything looser would widen what counts as a SPACE install.
    let checks = PeerProcessChecks {
        binary_paths: &["/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    for exe in [
        "/space/apps/space-sbxmgr-svc-1A2B3C4D",   // uppercase digest
        "/space/apps/space-sbxmgr-svc-1a2b3c",     // digest too short
        "/space/apps/space-sbxmgr-svc-1a2b3c4d5e", // digest too long
        "/space/apps/space-sbxmgr-svc2-1a2b3c4d",  // different binary name
        "/space/apps/space-sbxmgr-svc",            // no digest suffix
        "/space/apps/evil/space-sbxmgr-svc-1a2b3c4d", // nested directory
        "/space/apps2/space-sbxmgr-svc-1a2b3c4d",  // different directory
    ] {
        let snap = snapshot(exe, 1001, 0);
        let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new())
            .expect_err(&format!("{exe} must not match the allowlist"));
        assert!(matches!(err, ProcessCheckFailure::BinaryNotAllowed(_)));
    }
}

#[test]
fn evaluate_admits_a_check_time_resolved_symlink_target() {
    // An install layout whose symlink target sits outside `/space/apps` is
    // admitted only through the caller-resolved target list, and only for
    // the exact target.
    let checks = PeerProcessChecks {
        binary_paths: &["/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/somewhere/else/space-sbxmgr-svc", 1001, 0);

    let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new())
        .expect_err("an unresolved off-layout target must not match");
    assert!(matches!(err, ProcessCheckFailure::BinaryNotAllowed(_)));

    let resolved = vec![PathBuf::from("/somewhere/else/space-sbxmgr-svc")];
    evaluate_process_checks(&checks, 1001, &snap, |_| resolved)
        .expect("the entry's resolved symlink target must match");
}

#[cfg(target_os = "linux")]
#[test]
fn real_proc_read_reaches_the_binary_allowlist() {
    // End-to-end through the real /proc read: use the snapshot's own
    // effective uid so the pid-reuse guard cannot fire, pinning that the
    // decision then consults the binary allowlist. Runs as any uid,
    // including CI root.
    let snapshot =
        read_peer_process(std::process::id() as i32).expect("read this process's own /proc");
    let checks = PeerProcessChecks {
        binary_paths: &["/no/such/space-binary"],
        ..PeerProcessChecks::EMPTY
    };
    let err = evaluate_process_checks(&checks, snapshot.effective_uid, &snapshot, |_| Vec::new())
        .expect_err("the test binary is not in the allowlist");
    assert!(
        matches!(err, ProcessCheckFailure::BinaryNotAllowed(_)),
        "{err}"
    );

    // The accept half: allowlist the binary this process actually is.
    let own_exe: &'static str =
        Box::leak(snapshot.exe.to_string_lossy().into_owned().into_boxed_str());
    let own_list: &'static [&'static str] = Box::leak(Box::new([own_exe]));
    let checks = PeerProcessChecks {
        binary_paths: own_list,
        ..PeerProcessChecks::EMPTY
    };
    evaluate_process_checks(&checks, snapshot.effective_uid, &snapshot, |_| Vec::new())
        .expect("this process's own exe is allowlisted");
}

#[test]
fn symlink_resolution_is_skipped_when_a_string_form_matches() {
    // The resolver does filesystem reads, so the authorized fast path (a
    // literal or content-addressed match) must never invoke it.
    let checks = PeerProcessChecks {
        binary_paths: &["/opt/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/opt/space/bin/space-sbxmgr-svc", 1001, 0);
    evaluate_process_checks(&checks, 1001, &snap, |_| {
        unreachable!("a literal match must not resolve symlinks")
    })
    .expect("literal match authorizes without touching the filesystem");
}

#[cfg(unix)]
#[test]
fn resolve_binary_allowlist_targets_follows_symlinks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let apps = dir.path().join("apps");
    let bin = dir.path().join("bin");
    std::fs::create_dir_all(&apps).expect("apps dir");
    std::fs::create_dir_all(&bin).expect("bin dir");
    let target = apps.join("space-test-svc-1a2b3c4d");
    std::fs::write(&target, b"#!/bin/false\n").expect("target file");
    let link = bin.join("space-test-svc");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");

    let resolved = resolve_binary_allowlist_targets(&[
        link.to_str().expect("utf-8 path"),
        "/no/such/space-entry",
    ]);
    // Canonicalize the expectation too: the tempdir itself may sit behind a
    // platform symlink (macOS /tmp).
    assert_eq!(resolved, vec![target.canonicalize().expect("canonical")]);
}

#[test]
fn evaluate_rejects_disallowed_binary() {
    let checks = PeerProcessChecks {
        binary_paths: &["/opt/space/bin/space-sbxmgr-svc"],
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/usr/bin/bash", 1001, 0);
    let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new()).unwrap_err();
    assert!(matches!(err, ProcessCheckFailure::BinaryNotAllowed(_)));
}

#[test]
fn evaluate_forbidden_caps_must_all_be_absent() {
    let checks = PeerProcessChecks {
        forbidden_caps: capability::CAP_SYS_PTRACE,
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/x", 1001, capability::CAP_SYS_PTRACE);
    let err = evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new()).unwrap_err();
    let ProcessCheckFailure::HoldsForbiddenCapabilities(held) = err else {
        panic!("got {err:?}");
    };
    assert_eq!(held, capability::CAP_SYS_PTRACE);
}

#[test]
fn evaluate_forbidden_caps_pass_when_absent() {
    let checks = PeerProcessChecks {
        forbidden_caps: capability::CAP_SYS_PTRACE,
        ..PeerProcessChecks::EMPTY
    };
    let snap = snapshot("/x", 1001, capability::CAP_NET_ADMIN);
    evaluate_process_checks(&checks, 1001, &snap, |_| Vec::new()).expect("forbidden cap not held");
}

// --- UdsPeerAccount builders -------------------------------------------

#[test]
fn account_builders_compose() {
    let acct = UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR)
        .require_binary(&["/opt/space/bin/space-sbxmgr-svc"])
        .forbid_capabilities(capability::CAP_SYS_PTRACE);
    assert_eq!(acct.process_checks.binary_paths.len(), 1);
    assert_eq!(
        acct.process_checks.forbidden_caps,
        capability::CAP_SYS_PTRACE
    );
    assert!(acct.process_checks.is_configured());
}

#[test]
fn unconfigured_checks_are_not_configured() {
    let acct = UdsPeerAccount::with_linux_user(users::SPACE_SBXMGR);
    assert!(!acct.process_checks.is_configured());
}

// --- PeerProcessCheckMode (env kill switch) ----------------------------

#[test]
fn process_check_mode_parse_normalizes_whitespace_and_case() {
    assert_eq!(
        " ENFORCE ".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::Enforce
    );
    assert_eq!(
        "Log-Only".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::LogOnly
    );
    assert_eq!(
        " off\n".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::Off
    );
    "nonsense".parse::<PeerProcessCheckMode>().unwrap_err();
}

#[test]
fn process_check_mode_default_is_off() {
    // Off by default keeps local dev / non-Linux hosts working; deployments
    // opt in to log-only/enforce via PEER_PROCESS_CHECK_ENV.
    assert_eq!(PeerProcessCheckMode::default(), PeerProcessCheckMode::Off);
}

#[test]
fn only_the_canonical_mode_spellings_parse() {
    // The switch arms a security control; loose spellings invite false
    // confidence. `kill` reading as `off` is the poster child: it says
    // "kill the RPC" and disables enforcement.
    assert_eq!(
        "enforce".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::Enforce
    );
    assert_eq!(
        "log-only".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::LogOnly
    );
    assert_eq!(
        "off".parse::<PeerProcessCheckMode>().unwrap(),
        PeerProcessCheckMode::Off
    );
    for synonym in [
        "enforcing",
        "armed",
        "arm",
        "on",
        "log_only",
        "logging-only",
        "log",
        "observe",
        "disabled",
        "disable",
        "kill",
    ] {
        assert!(
            synonym.parse::<PeerProcessCheckMode>().is_err(),
            "`{synonym}` must not parse to a mode"
        );
    }
}

#[test]
fn unparseable_env_mode_must_fail_not_silently_disarm() {
    // A typo'd value ("enfroce") must be a startup error: falling back to
    // Off would disarm the control the variable was set to arm.
    let err = PeerProcessCheckMode::from_env_value(Ok("enfroce".to_string()))
        .expect_err("a typo'd mode value must fail, not silently disarm");
    assert!(matches!(err, PeerProcessCheckModeEnvError::Invalid(_)));
}

#[test]
fn env_mode_mapping_is_strict() {
    // The from_env mapping, via the injected-read seam so no test mutates
    // process-global env state.
    assert_eq!(
        PeerProcessCheckMode::from_env_value(Ok("enforce".to_string())).unwrap(),
        PeerProcessCheckMode::Enforce
    );
    assert_eq!(
        PeerProcessCheckMode::from_env_value(Ok("log-only".to_string())).unwrap(),
        PeerProcessCheckMode::LogOnly
    );
    assert_eq!(
        PeerProcessCheckMode::from_env_value(Ok("off".to_string())).unwrap(),
        PeerProcessCheckMode::Off
    );
    // Absent means the dev-safe default, not an error.
    assert_eq!(
        PeerProcessCheckMode::from_env_value(Err(std::env::VarError::NotPresent)).unwrap(),
        PeerProcessCheckMode::Off
    );
}

#[cfg(unix)]
#[test]
fn non_unicode_env_mode_is_an_error() {
    use std::os::unix::ffi::OsStringExt;

    let raw = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
    let err = PeerProcessCheckMode::from_env_value(Err(std::env::VarError::NotUnicode(raw)))
        .expect_err("non-unicode bytes cannot name a mode");
    assert!(matches!(err, PeerProcessCheckModeEnvError::NotUnicode));
}

// --- kill switch end-to-end through authorize_identity -----------------
//
// These drive the real `/proc` read using the test process's own pid. The
// configured allowlist never matches the test binary, so the process check
// always *fails* (binary-not-allowed or uid-drift on Linux,
// `/proc`-unsupported on non-Linux). What differs per mode is the verdict.

const PEER_UID: u32 = 4242;

fn configured_authorizer(mode: PeerProcessCheckMode) -> UdsPeerAuthorizer {
    UdsPeerAuthorizer::new(
        "test-service",
        [UdsPeerAccount::with_fallback_uid("test-peer", PEER_UID)
            .require_binary(&["/no/such/space-binary"])],
    )
    .with_process_check_mode(mode)
}

fn peer_identity_for_self() -> PeerIdentity {
    PeerIdentity::new(PEER_UID, PEER_UID, Some(std::process::id() as i32))
}

#[test]
fn kill_switch_off_skips_failing_process_check() {
    configured_authorizer(PeerProcessCheckMode::Off)
        .authorize_identity(
            peer_identity_for_self(),
            "test_op",
            &[UdsPeerPrincipal::Service("test-peer")],
        )
        .expect("Off skips process checks; SO_PEERCRED match alone authorizes");
}

#[test]
fn log_only_allows_despite_failing_process_check() {
    configured_authorizer(PeerProcessCheckMode::LogOnly)
        .authorize_identity(
            peer_identity_for_self(),
            "test_op",
            &[UdsPeerPrincipal::Service("test-peer")],
        )
        .expect("LogOnly warns but allows");
}

#[test]
fn enforce_rejects_failing_process_check() {
    let err = configured_authorizer(PeerProcessCheckMode::Enforce)
        .authorize_identity(
            peer_identity_for_self(),
            "test_op",
            &[UdsPeerPrincipal::Service("test-peer")],
        )
        .unwrap_err();
    // PermissionDenied on Linux (binary/uid-drift); Unauthenticated on a
    // host without `/proc`. Both reject the RPC.
    assert!(
        matches!(
            err.code(),
            tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
        ),
        "unexpected code: {:?}",
        err.code()
    );
}

#[test]
fn process_checks_are_skipped_for_peers_without_an_account() {
    // Root and unclassified peers never match an account, so there is no
    // per-account `PeerProcessChecks` to apply — even under enforce.
    let auth = configured_authorizer(PeerProcessCheckMode::Enforce);
    let pid = Some(std::process::id() as i32);
    auth.authorize_identity(
        PeerIdentity::new(0, 0, pid),
        "test_op",
        &[UdsPeerPrincipal::Root],
    )
    .expect("root has no account to check");
    auth.authorize_identity(
        PeerIdentity::new(31_337, 31_337, pid),
        "test_op",
        &[UdsPeerPrincipal::Other],
    )
    .expect("an unclassified peer has no account to check");
}

#[test]
fn process_checks_are_skipped_for_account_without_checks() {
    let auth = UdsPeerAuthorizer::new(
        "test-service",
        [UdsPeerAccount::with_fallback_uid("test-peer", PEER_UID)],
    )
    .with_process_check_mode(PeerProcessCheckMode::Enforce);

    auth.authorize_identity(
        peer_identity_for_self(),
        "test_op",
        &[UdsPeerPrincipal::Service("test-peer")],
    )
    .expect("an account that declared no checks is never inspected");
}

#[test]
fn require_local_authority_peer_is_the_shared_operator_boundary() {
    // authority=cluster is always allowed, regardless of principal.
    for principal in [
        UdsPeerPrincipal::Root,
        UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
        UdsPeerPrincipal::Role(LocalRole::Observer),
        UdsPeerPrincipal::Other,
    ] {
        require_local_authority_peer(principal, false)
            .expect("cluster authority is always allowed");
    }

    // authority=local: only on-node operators (root / Operator+).
    assert!(peer_may_set_local_authority(UdsPeerPrincipal::Root));
    assert!(peer_may_set_local_authority(UdsPeerPrincipal::Role(
        LocalRole::Operator
    )));
    assert!(peer_may_set_local_authority(UdsPeerPrincipal::Role(
        LocalRole::Admin
    )));
    require_local_authority_peer(UdsPeerPrincipal::Root, true).expect("root may set local");
    require_local_authority_peer(UdsPeerPrincipal::Role(LocalRole::Operator), true)
        .expect("operator may set local");

    for principal in [
        UdsPeerPrincipal::Service(user_names::SPACE_NODEAPI),
        UdsPeerPrincipal::Service(user_names::SPACE_NODECTL),
        UdsPeerPrincipal::Role(LocalRole::Observer),
        UdsPeerPrincipal::Other,
    ] {
        assert!(!peer_may_set_local_authority(principal));
        assert_eq!(
            require_local_authority_peer(principal, true)
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}
