//! One delivery through nftables in disposable network namespaces. Needs
//! root, iproute2 and nftables; listed in the nextest privileged group.

use std::fs::File;
use std::sync::Mutex;

use pplx_space_node_runtime_core::proto::LaunchRuntimeResponse;

use super::*;
use crate::firewall::rules::{PRIORITY, render};
use crate::netns_tests::{TestNamespace, contained, inside, link_guest};
use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
use crate::slot_pool::firewall_rules::tests::linux::{Redirected, list, owned_table};

// Catches a delivery that misses the bound slot's namespace or touches another.
#[test]
fn a_delivery_applies_only_to_the_bound_namespace() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let target = TestNamespace::create();
    let sibling = TestNamespace::create();
    let sibling_before = list(&sibling, &["-j", "list", "ruleset"]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    // A blocking thread the runtime starts inherits this contained namespace.
    let replied = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
                set_slot(&service.pool, SLOT, SlotState::Leased, 1);
                let service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slots: BoundSlots::One(SLOT),
                        assignment_path: path.clone(),
                        host: Arc::new(Redirected(vec![
                            sibling.name().to_string(),
                            target.name().to_string(),
                        ])),
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                    })
                    .unwrap();
                reply(sync(&service, delivery(12)).await)
            })
    });
    let committed = FirewallAssignmentAcceptance::Committed;
    assert_eq!(replied, (12, committed, Outcome::Matched));
    let reopened = FirewallAssignments::open(&path).unwrap();
    assert_eq!(reopened.current().unwrap().unwrap().generation, 12);
    assert_eq!(owned_table(&target), installed(12));
    assert_eq!(list(&sibling, &["-j", "list", "ruleset"]), sibling_before);
}

// Catches a node-wide delivery that misses a leased or the idle slot's
// namespace, or touches an ineligible one.
#[test]
fn a_delivery_applies_to_every_eligible_namespace() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let eligible = [(); 3].map(|()| TestNamespace::create());
    let dirty = TestNamespace::create();
    let dirty_before = list(&dirty, &["-j", "list", "ruleset"]);
    let names = eligible
        .iter()
        .chain([&dirty])
        .map(|namespace| namespace.name().to_string());
    let host = Arc::new(Redirected(names.collect()));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    // A blocking thread the runtime starts inherits this contained namespace.
    let passed = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(4));
                let states = [SlotState::Leased, SlotState::Leased, SlotState::Ready];
                for (slot, state) in (0..).zip(states.into_iter().chain([SlotState::Dirty])) {
                    set_slot(&service.pool, slot, state, 1);
                }
                let service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slots: BoundSlots::All,
                        assignment_path: path.clone(),
                        host,
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                    })
                    .unwrap();
                sync_all(&service, 12).await;
                passed(&service, 12).await
            })
    });
    let matched = Some(Outcome::Matched);
    assert_eq!(passed.unwrap(), [(0, matched), (1, matched), (2, matched)]);
    for namespace in &eligible {
        assert_eq!(owned_table(namespace), installed(12), "{namespace:?}");
    }
    assert_eq!(list(&dirty, &["-j", "list", "ruleset"]), dirty_before);
}

/// Each counter in the probe table, by comment.
fn counters(namespace: &TestNamespace) -> std::collections::HashMap<String, u64> {
    let listed = list(namespace, &["-j", "list", "table", "ip", "probe"]);
    let rules = listed["nftables"].as_array().unwrap().iter();
    let counted = rules.filter_map(|object| {
        let rule = object.get("rule")?;
        let counter = rule["expr"]
            .as_array()?
            .iter()
            .find_map(|e| e.get("counter"))?;
        Some((
            rule["comment"].as_str()?.to_string(),
            counter["packets"].as_u64()?,
        ))
    });
    counted.collect()
}

// Catches a start whose guest's first packets meet the namespace's earlier
// table, not the accepted assignment: port 12 is open under 11 and closed under 12.
#[test]
fn a_start_s_first_guest_traffic_meets_the_accepted_assignment() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    // Slot 0 is leased during 12's sweep; slot 1 is dirty then, and keeps 11.
    let (leased, fresh, guest) = (
        TestNamespace::create(),
        TestNamespace::create(),
        TestNamespace::create(),
    );
    let (f, g) = (fresh.name(), guest.name());
    // A veth named tap0 replaces the dummy, so the guest side can send.
    link_guest(&fresh, &guest);
    let entry = format!("/run/netns/{f}");
    let netns = File::open(&entry).unwrap();
    let budget = crate::host_command::PROBE_BUDGET;
    let earlier = pplx_space_firewall_rules_protocol::decode_revision(&document(11)).unwrap();
    let outcome = contained(|| crate::firewall::rules::install::install(&render(&earlier), &netns));
    assert!(matches!(outcome, InstallOutcome::Committed), "{outcome:?}");
    // Counts tap0 packets just before and just after the owned chain.
    let (before, after) = (PRIORITY - 1, PRIORITY + 1);
    let probe = format!(
        r#"table ip probe {{
        chain before {{ type filter hook prerouting priority {before};
            iifname "tap0" tcp dport 12 counter comment "before-12"
            iifname "tap0" tcp dport 13 counter comment "before-13"
        }}
        chain after {{ type filter hook prerouting priority {after};
            iifname "tap0" tcp dport 12 counter comment "after-12"
            iifname "tap0" tcp dport 13 counter comment "after-13"
        }}
    }}"#
    );
    let added =
        contained(|| crate::firewall::rules::nft(&netns, &["-f", "-"], probe.as_bytes(), budget));
    assert!(added.unwrap().status.success());
    let control = inside(f, || std::net::TcpListener::bind("10.0.0.1:13").unwrap());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("assignment.json");
    let host = Arc::new(Redirected(vec![leased.name().to_string(), f.to_string()]));
    let launches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let launched = launches.clone();
    let guest = g.to_string();
    // A blocking thread the runtime starts inherits this contained namespace.
    let connected = contained(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
            .block_on(async {
                let mut service = service_for_unit_tests().await;
                service.pool = NetworkPool::open_in_memory_for_test(NetworkPoolConfig::new(2));
                set_slot(&service.pool, 0, SlotState::Leased, 1);
                set_slot(&service.pool, 1, SlotState::Dirty, 1);
                let mut service = service
                    .with_firewall_sync(FirewallSyncFixture {
                        node_id: NODE,
                        slots: BoundSlots::All,
                        assignment_path: path.clone(),
                        host,
                        nft: Arc::new(crate::firewall::rules::nft),
                        wait: WAIT,
                    })
                    .unwrap();
                sync_all(&service, 12).await;
                assert_eq!(
                    passed(&service, 12).await.unwrap(),
                    [(0, Some(Outcome::Matched))]
                );
                set_slot(&service.pool, 1, SlotState::Ready, 1);
                let lease = super::start::pooled(&service).await;
                assert_eq!(lease.slot, 1);
                // The guest's first packets: the denied SYN, then the control.
                let (connected, sent) = (Arc::new(Mutex::new(None)), launched);
                let reached = connected.clone();
                let launch = move |_| {
                    sent.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let reply = inside(&guest, || {
                        use socket2::{Domain, Socket, Type};
                        let denied = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
                        denied.set_nonblocking(true).unwrap();
                        let target = std::net::SocketAddr::from(([10, 0, 0, 1], 12));
                        let _ = denied.connect(&target.into());
                        let target = std::net::SocketAddr::from(([10, 0, 0, 1], 13));
                        let allowed = std::net::TcpStream::connect_timeout(&target, BOUND);
                        (denied, allowed.map(drop).map_err(|error| error.kind()))
                    });
                    *reached.lock().unwrap() = Some(reply);
                    Ok(LaunchRuntimeResponse::default())
                };
                service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
                    launch: Some(Arc::new(launch)),
                    ..Default::default()
                });
                super::start::start(&service, &lease, Path::new(&entry))
                    .await
                    .unwrap();
                let (_denied, allowed) = connected.lock().unwrap().take().unwrap();
                allowed
            })
    });
    assert_eq!(connected, Ok(()));
    assert_eq!(launches.load(std::sync::atomic::Ordering::SeqCst), 1);
    control.accept().unwrap();
    let counted = counters(&fresh);
    assert!(counted["before-12"] >= 1, "{counted:?}");
    assert_eq!(counted["after-12"], 0, "{counted:?}");
    assert!(counted["after-13"] >= 1, "{counted:?}");
    assert_eq!(owned_table(&fresh), installed(12));
}
