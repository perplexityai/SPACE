use std::io;

#[test]
fn network_teardown_attempts_independent_cleanup_and_requires_both() {
    for (links_ok, ingress_ok) in [(true, true), (false, true), (true, false), (false, false)] {
        let mut attempted = false;
        let outcome = |ok| {
            if ok {
                Ok(())
            } else {
                Err(super::NetworkTeardownError::Remaining {
                    resource: "fixture".into(),
                })
            }
        };
        let result = super::finish_network_teardown(
            || outcome(links_ok),
            || {
                attempted = true;
                outcome(ingress_ok)
            },
        );
        assert!(attempted);
        assert_eq!(result.is_ok(), links_ok && ingress_ok);
    }
}

/// The pod-namespace teardown steps finish only when the namespace's
/// /run/netns entry is gone or no longer names a namespace, as the probe and
/// nft paths report it.
#[cfg(target_os = "linux")]
#[test]
fn pod_namespace_step_is_done_only_when_the_namespace_entry_is_gone_or_unmounted() {
    use super::{NetworkProbeError, NetworkTeardownError, finish_pod_namespace_step};
    use crate::firewall::FirewallError;

    let gone = format!("space-test-gone-{}", std::process::id());
    let probe = super::try_link_exists_in("sveth0", Some(&gone)).unwrap_err();
    assert!(
        matches!(probe, NetworkProbeError::NamespaceMissing { .. }),
        "{probe:?}"
    );
    // nft runs in the namespace through this path; `true` keeps the fixture
    // read-only should that namespace ever exist.
    let ingress = FirewallError::Exec(super::command_output_in_ns(&gone, "true", &[]).unwrap_err());
    assert!(ingress.is_namespace_missing(), "{ingress:?}");
    let unmounted = || NetnsEnterError::TargetUnmounted {
        path: format!("/run/netns/{gone}"),
    };
    for error in [
        NetworkTeardownError::Probe(probe),
        NetworkTeardownError::Ingress(ingress),
        NetworkTeardownError::Ingress(FirewallError::Exec(io::Error::from(unmounted()))),
        NetworkTeardownError::Ingress(FirewallError::ListStart(io::Error::from(unmounted()))),
    ] {
        finish_pod_namespace_step(|| Err(error)).unwrap();
    }

    let target = |errno| NetnsEnterError::OpenTarget {
        path: format!("/run/netns/{gone}"),
        source: io::Error::from_raw_os_error(errno),
    };
    for error in [
        NetworkTeardownError::Probe(NetworkProbeError::Namespace {
            ns: gone.clone(),
            detail: "failed to switch network namespace".into(),
        }),
        NetworkTeardownError::Ingress(FirewallError::ListStart(io::Error::from(target(
            libc::EACCES,
        )))),
        // setns refusing a file that does name a namespace, EINVAL included.
        NetworkTeardownError::Ingress(FirewallError::Exec(io::Error::from(
            NetnsEnterError::SetNs {
                detail: "failed to switch network namespace: EINVAL: Invalid argument".into(),
            },
        ))),
        // nft itself missing is not a missing namespace.
        NetworkTeardownError::Ingress(FirewallError::Exec(io::Error::from_raw_os_error(
            libc::ENOENT,
        ))),
        NetworkTeardownError::Remaining {
            resource: "interface sveth0".into(),
        },
    ] {
        let context = format!("{error:?}");
        finish_pod_namespace_step(|| Err(error)).expect_err(&context);
    }
}

/// Removing a named namespace unmounts its /run/netns entry before unlinking
/// it. An entry opened in between is a plain file that names no namespace,
/// so entering it counts as a missing namespace; a namespace file that
/// cannot be entered does not.
#[cfg(target_os = "linux")]
#[test]
fn an_unmounted_namespace_entry_counts_as_missing() {
    let placeholder = tempfile::NamedTempFile::new().unwrap();
    let path = placeholder.path().display().to_string();
    let error = super::native::enter_netns_file(&path, placeholder.as_file()).unwrap_err();
    assert!(
        matches!(error, NetnsEnterError::TargetUnmounted { .. }),
        "{error:?}"
    );
    assert!(error.is_target_missing(), "{error:?}");
    let in_ns = io::Error::from(error);
    assert!(super::is_namespace_missing_io_error(&in_ns), "{in_ns:?}");

    // Unprivileged, entering a namespace is refused; as root this thread
    // re-enters its own namespace, which changes nothing.
    std::thread::spawn(|| {
        let own = std::fs::File::open("/proc/thread-self/ns/net").unwrap();
        if let Err(error) = super::native::enter_netns_file("/proc/thread-self/ns/net", &own) {
            assert!(!error.is_target_missing(), "{error:?}");
        }
        // A namespace of another type is refused with EINVAL, or EPERM under
        // a seccomp filter, and is still not a missing namespace.
        let uts = std::fs::File::open("/proc/thread-self/ns/uts").unwrap();
        let error = super::native::enter_netns_file("/proc/thread-self/ns/uts", &uts).unwrap_err();
        assert!(matches!(error, NetnsEnterError::SetNs { .. }), "{error:?}");
    })
    .join()
    .unwrap();
}

use crate::net::{
    DefaultIfaceError, NetnsEnterError, NetworkOpError, NetworkSetupDisposition, NetworkSetupError,
    classify_firewall_setup_error, is_slot_vpeer_ip,
};

#[test]
fn slot_vpeer_ip_predicate_matches_slot_address_range() {
    assert!(is_slot_vpeer_ip("10.12.128.2".parse().unwrap()));
    assert!(is_slot_vpeer_ip("10.12.255.254".parse().unwrap()));

    assert!(!is_slot_vpeer_ip("10.12.128.1".parse().unwrap()));
    assert!(!is_slot_vpeer_ip("10.12.128.3".parse().unwrap()));
    assert!(!is_slot_vpeer_ip("10.12.255.255".parse().unwrap()));
    assert!(!is_slot_vpeer_ip("10.13.0.0".parse().unwrap()));
}

#[test]
fn io_error_classification_drives_setup_disposition() {
    for kind in [
        io::ErrorKind::TimedOut,
        io::ErrorKind::Interrupted,
        io::ErrorKind::WouldBlock,
    ] {
        assert!(
            classify_firewall_setup_error(crate::firewall::FirewallError::Exec(io::Error::from(
                kind
            ),))
            .is_transient(),
            "{kind:?} must retain the bounded setup retry policy"
        );
    }
    let retryable = NetworkSetupError::HostVeth {
        source: NetworkOpError::from_io_error(io::Error::from_raw_os_error(libc::EMFILE)),
    };
    assert_eq!(
        retryable.disposition(),
        NetworkSetupDisposition::RetryWithCap
    );

    let permanent = NetworkSetupError::HostVeth {
        source: NetworkOpError::from_io_error(io::Error::from_raw_os_error(libc::EACCES)),
    };
    assert_eq!(permanent.disposition(), NetworkSetupDisposition::Quarantine);
}

#[test]
fn netns_enter_enoent_classifies_as_missing_namespace() {
    let missing = DefaultIfaceError::from(NetnsEnterError::OpenTarget {
        path: "/run/netns/cni-0123".to_string(),
        source: io::Error::from_raw_os_error(libc::ENOENT),
    });
    assert!(matches!(
        missing,
        DefaultIfaceError::NetnsMissing { ref path, .. } if path == "/run/netns/cni-0123"
    ));

    // A namespace that exists but cannot be opened is not "missing".
    let denied = DefaultIfaceError::from(NetnsEnterError::OpenTarget {
        path: "/run/netns/cni-0123".to_string(),
        source: io::Error::from_raw_os_error(libc::EACCES),
    });
    assert!(matches!(denied, DefaultIfaceError::Other(_)));

    let panicked = DefaultIfaceError::from(NetnsEnterError::ThreadPanicked {
        ns: "cni-0123".to_string(),
    });
    assert!(matches!(panicked, DefaultIfaceError::Other(_)));
}

#[test]
fn netns_enter_failures_stay_transient_for_setup_paths() {
    // Setup/teardown paths retry with cap on entry failures; the typed
    // refactor must not silently flip them to quarantine.
    let error = NetworkOpError::from(NetnsEnterError::OpenTarget {
        path: "/run/netns/cni-0123".to_string(),
        source: io::Error::from_raw_os_error(libc::ENOENT),
    });
    assert!(error.is_transient());
}

#[test]
fn firewall_setup_classifier_preserves_transient_and_permanent_sources() {
    assert!(
        !classify_firewall_setup_error(crate::firewall::FirewallError::MissingRuleHandle {
            comment: "ct-inbound-replies",
        })
        .is_transient()
    );
    assert!(
        classify_firewall_setup_error(crate::firewall::FirewallError::ListStart(
            io::Error::from_raw_os_error(libc::EMFILE),
        ))
        .is_transient()
    );
    assert!(
        classify_firewall_setup_error(crate::firewall::FirewallError::ListFailed {
            stderr: "netlink operation timed out".to_string(),
        })
        .is_transient()
    );
    assert!(
        classify_firewall_setup_error(crate::firewall::FirewallError::ReadPreTproxyPolicy {
            path: "policy.json",
            source: io::Error::from_raw_os_error(libc::ENOMEM),
        },)
        .is_transient()
    );
    assert!(
        classify_firewall_setup_error(crate::firewall::FirewallError::Apply {
            stderr: "netlink operation timed out".to_string(),
            input: "{}".to_string(),
        })
        .is_transient()
    );

    assert!(
        !classify_firewall_setup_error(crate::firewall::FirewallError::Apply {
            stderr: "rule rejected by kernel".to_string(),
            input: "{}".to_string(),
        })
        .is_transient()
    );
    assert!(
        !classify_firewall_setup_error(crate::firewall::FirewallError::ReadPreTproxyPolicy {
            path: "policy.json",
            source: io::Error::from_raw_os_error(libc::EACCES),
        },)
        .is_transient()
    );
}
