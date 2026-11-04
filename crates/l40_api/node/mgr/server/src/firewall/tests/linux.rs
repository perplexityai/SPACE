//! Root only, and needs nft and iproute2. Each test runs in a network
//! namespace its thread unshares, so it changes nothing on the host.

use super::*;

/// Run `test` on a thread whose network namespace is new and empty. Every
/// child it starts, nft included, inherits that namespace.
fn in_private_network_namespace(test: impl FnOnce() + Send + 'static) {
    let thread = std::thread::spawn(move || {
        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET).unwrap();
        let namespace = |path| std::fs::read_link(path).unwrap();
        assert_ne!(
            namespace("/proc/thread-self/ns/net"),
            namespace("/proc/self/ns/net"),
            "refusing to change the process's own network namespace"
        );
        test();
    });
    if let Err(panic) = thread.join() {
        std::panic::resume_unwind(panic);
    }
}

/// The namespace's ruleset, and how many stamps it holds in all.
fn list_ruleset() -> (NamespaceRuleset, usize) {
    let output = nft_output(None, &["-j", "list", "ruleset"]).unwrap();
    assert!(output.status.success(), "{output:?}");
    let listing: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let stamps = listing["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| config_stamp::is_stamp_rule(rule))
        .count();
    (NamespaceRuleset::parse(&output.stdout).unwrap(), stamps)
}

#[test]
fn full_install_lists_every_stamp_as_written() {
    if !pplx_space_test_env::root_or_skip("install nftables rules in a private network namespace") {
        return;
    }
    in_private_network_namespace(|| {
        let tap = std::process::Command::new("ip")
            .args(["link", "add", "tap0", "type", "dummy"])
            .status()
            .unwrap();
        assert!(tap.success());
        let policy = packaged_pre_tproxy_guardrails_policy();
        let install = full_install("vpeer-000000", "10.12.128.2", 12345, false, &policy);
        let (script, documents) = &install;
        apply_nft_script(script, None).unwrap();
        for document in documents {
            apply_nft_json(&document.to_string(), None).unwrap();
        }
        // Startup re-applies the L2 policy and each gateway launch the proxy
        // rules. Both flush their chains, so each stamp is replaced, not added.
        apply_nft_script(script, None).unwrap();
        apply_nft_json(&netgtw_tproxy_rules_json(12345, false), None).unwrap();

        let (ruleset, stamp_count) = list_ruleset();
        let mut listed = std::collections::HashMap::new();
        for part in every_part() {
            let (family, table, chain) = part.stamp_chain();
            let chain = ruleset.chain(family, table, chain).unwrap();
            let rules = chain["nftables"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|entry| entry.get("rule"))
                .collect::<Vec<_>>();
            let (stamps, rest): (Vec<_>, Vec<_>) = rules
                .iter()
                .enumerate()
                .partition(|(_, rule)| config_stamp::is_stamp_rule(rule));
            assert!(
                stamps
                    .iter()
                    .all(|(position, _)| rest.iter().all(|(other, _)| other < position)),
                "{part}: a stamp precedes a rule of its chain"
            );
            let comment = stamps
                .iter()
                .map(|(_, rule)| rule["comment"].as_str().unwrap())
                .find(|comment| comment.starts_with(&format!("space-cfg:{part}=")));
            listed.insert(part, comment.unwrap().to_string());
        }
        assert_eq!(listed, written_stamps(&install));
        assert_eq!(
            stamp_count,
            every_part().len(),
            "a rebuilt part kept its old stamp"
        );

        // The namespace checks accept a stamped namespace as nft lists it.
        let chain = |family, table, name| ruleset.chain(family, table, name).unwrap();
        validate_guest_l2_ingress_chain(&chain("netdev", SANDBOX_L2_TABLE, "ingress")).unwrap();
        let native = nft_output(
            None,
            &["-n", "list", "chain", "netdev", SANDBOX_L2_TABLE, "ingress"],
        )
        .unwrap();
        validate_guest_l2_ingress_native_binding(&String::from_utf8_lossy(&native.stdout)).unwrap();
        let packaged: Vec<serde_json::Value> =
            serde_json::from_str(&format!("[{policy}]")).unwrap();
        validate_pre_tproxy_guardrails_chain(
            &chain("ip", SANDBOX_TABLE, PRE_TPROXY_GUARDRAILS_CHAIN),
            "169.254.0.21",
            &packaged,
        )
        .unwrap();
        let forward = chain("ip", SANDBOX_TABLE, "forward");
        validate_forward_drop_chain(&forward, "vpeer-000000").unwrap();
        assert!(
            inbound_reply_rule_removals(Some(&forward))
                .unwrap()
                .is_empty()
        );
        let proxy = chain("ip", PROXY_TABLE, "prerouting");
        assert!(
            local_service_reply_rule_updates(Some(&proxy))
                .unwrap()
                .is_empty()
        );
    });
}
