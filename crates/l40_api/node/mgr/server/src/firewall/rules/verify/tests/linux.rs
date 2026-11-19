//! Readback from nftables in disposable network namespaces. Needs root,
//! iproute2 and nftables; listed in the nextest privileged group.
use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::time::Duration;

use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::decode_revision;

use super::super::{Verification, verify};
use crate::firewall::rules::install::{InstallOutcome, install};
use crate::firewall::rules::{TableConstruction, render};
use crate::netns_tests::{TestNamespace, contained, ip};

const PRIVILEGE: &str = "create network namespaces and nftables tables";
const DNS: &str = r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]}"#;
const WEB: &str = r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":8080}]}"#;

fn decoded(rules: &[&str]) -> FirewallRuleRevision {
    let rules = rules.join(",");
    decode_revision(&format!(r#"{{"schema_version":1,"rules":[{rules}]}}"#)).unwrap()
}

fn pinned(namespace: &TestNamespace) -> File {
    File::open(format!("/run/netns/{}", namespace.name())).unwrap()
}

fn committed(construction: &TableConstruction, netns: &File) {
    let outcome = contained(|| install(construction, netns));
    assert!(matches!(outcome, InstallOutcome::Committed), "{outcome:?}");
}

/// Runs `f` on a thread inside `namespace`; sockets it opens stay there.
fn inside<T: Send>(namespace: &TestNamespace, f: impl FnOnce() -> T + Send) -> T {
    let netns = pinned(namespace);
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                nix::sched::setns(&netns, nix::sched::CloneFlags::CLONE_NEWNET).unwrap();
                f()
            })
            .join()
            .unwrap()
    })
}

// Catches a verifier that misreads a correct live table, depends on kernel
// handles, or matches an absent or differently ordered table.
#[test]
fn live_tables_match_only_their_revision() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let netns = pinned(&target);
    let empty = decoded(&[]);
    let both = decoded(&[DNS, WEB]);
    assert!(matches!(
        contained(|| verify(&empty, &netns)),
        Verification::Mismatch(_)
    ));

    committed(&render(&empty), &netns);
    assert_eq!(contained(|| verify(&empty, &netns)), Verification::Match);
    // Replacing the table gives it a new handle.
    committed(&render(&both), &netns);
    assert_eq!(contained(|| verify(&both, &netns)), Verification::Match);
    for other in [decoded(&[WEB, DNS]), empty] {
        let verification = contained(|| verify(&other, &netns));
        assert!(
            matches!(verification, Verification::Mismatch(_)),
            "{verification:?}"
        );
    }
}

// Catches a verifier that trusts the installer or the renderer: nft accepts
// the guardless rule, and only readback can reject it.
#[test]
fn guardless_rule_installed_by_the_installer_is_a_mismatch() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let netns = pinned(&target);
    let revision = decoded(&[DNS]);
    let mut construction = render(&revision);
    construction.0[2]["add"]["rule"]["expr"]
        .as_array_mut()
        .unwrap()
        .remove(0);
    committed(&construction, &netns);
    let verification = contained(|| verify(&revision, &netns));
    assert!(
        matches!(verification, Verification::Mismatch(_)),
        "{verification:?}"
    );
}

// Catches a verified table that does not enforce: a denied destination, an
// allowed one, and a flow established before installation, all through tap0.
#[test]
fn verified_table_drops_denied_and_established_traffic_only() {
    if !pplx_space_test_env::root_or_skip(PRIVILEGE) {
        return;
    }
    let target = TestNamespace::create();
    let guest = TestNamespace::create();
    // A veth named tap0 replaces the dummy, so the guest side can send.
    let (t, g) = (target.name(), guest.name());
    ip(&["-n", t, "link", "del", "tap0"]);
    ip(&[
        "-n", t, "link", "add", "tap0", "type", "veth", "peer", "name", "eth0", "netns", g,
    ]);
    ip(&["-n", t, "addr", "add", "10.0.0.1/24", "dev", "tap0"]);
    ip(&["-n", g, "addr", "add", "10.0.0.2/24", "dev", "eth0"]);
    ip(&["-n", t, "link", "set", "tap0", "up"]);
    ip(&["-n", g, "link", "set", "eth0", "up"]);

    let listener = inside(&target, || TcpListener::bind("10.0.0.1:8080").unwrap());
    let denied = inside(&target, || UdpSocket::bind("10.0.0.1:53").unwrap());
    let allowed = inside(&target, || UdpSocket::bind("10.0.0.1:54").unwrap());
    let sender = inside(&guest, || UdpSocket::bind("10.0.0.2:0").unwrap());
    let mut client = inside(&guest, || TcpStream::connect("10.0.0.1:8080").unwrap());
    client.set_nodelay(true).unwrap();
    let (mut server, _) = listener.accept().unwrap();
    for socket in [&denied, &allowed] {
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
    }
    server
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut buffer = [0; 16];

    // Both destinations and the flow work before installation.
    client.write_all(b"before").unwrap();
    server.read_exact(&mut buffer[..6]).unwrap();
    sender.send_to(b"before", "10.0.0.1:53").unwrap();
    assert_eq!(denied.recv(&mut buffer).unwrap(), 6);

    let revision = decoded(&[DNS, WEB]);
    committed(&render(&revision), &pinned(&target));
    assert_eq!(
        contained(|| verify(&revision, &pinned(&target))),
        Verification::Match
    );

    client.write_all(b"after").unwrap();
    sender.send_to(b"denied", "10.0.0.1:53").unwrap();
    sender.send_to(b"allowed", "10.0.0.1:54").unwrap();
    let received = allowed.recv(&mut buffer).unwrap();
    assert_eq!(&buffer[..received], b"allowed");
    // The allowed datagram left the same socket last over the same link, so
    // the denied one would have arrived by now.
    denied.set_nonblocking(true).unwrap();
    assert_eq!(
        denied.recv(&mut buffer).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    // TCP keeps no order with those datagrams, so wait for the segment instead:
    // delivered, it arrives well within the bound, retransmission included.
    server
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let error = server.read(&mut buffer).unwrap_err();
    assert!(
        matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
        "{error}"
    );
}
