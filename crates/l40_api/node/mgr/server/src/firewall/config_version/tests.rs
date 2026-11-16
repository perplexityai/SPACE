use super::*;

/// A packaged policy owned by these tests, so editing the image's policy file
/// cannot turn one of the mutations below into a no-op or a key insert.
const FIXTURE_POLICY: &str = r#"{ "add": { "rule": { "family": "ip", "table": "sandbox",
    "chain": "pre_tproxy_guardrails", "comment": "allow-established", "expr": [
      { "match": { "op": "==", "left": { "meta": { "key": "l4proto" } }, "right": "tcp" } },
      { "match": { "op": "in", "left": { "ct": { "key": "state" } },
        "right": { "set": ["established", "related"] } } },
      { "accept": null } ] } } },
{ "add": { "rule": { "family": "ip", "table": "sandbox",
    "chain": "pre_tproxy_guardrails", "comment": "drop-https", "expr": [
      { "match": { "op": "==", "left": { "meta": { "key": "l4proto" } }, "right": "tcp" } },
      { "match": { "op": "==", "left": { "meta": { "key": "mark" } }, "right": 0 } },
      { "match": { "op": "==", "left": { "payload": { "protocol": "tcp", "field": "dport" } },
        "right": 443 } },
      { "drop": null } ] } } },
{ "add": { "rule": { "family": "ip", "table": "sandbox",
    "chain": "pre_tproxy_guardrails", "comment": "drop-private", "expr": [
      { "match": { "op": "==", "left": { "meta": { "key": "l4proto" } }, "right": "udp" } },
      { "match": { "op": "==", "left": { "payload": { "protocol": "ip", "field": "daddr" } },
        "right": { "prefix": { "addr": "10.0.0.0", "len": 8 } } } },
      { "match": { "op": "==", "left": { "meta": { "key": "mark" } }, "right": 0 } },
      { "counter": null },
      { "drop": null } ] } } },
{ "add": { "rule": { "family": "ip", "table": "sandbox",
    "chain": "pre_tproxy_guardrails", "comment": "drop-link-local", "expr": [
      { "match": { "op": "==", "left": { "payload": { "protocol": "ip", "field": "daddr" } },
        "right": { "prefix": { "addr": "169.254.0.0", "len": 16 } } } },
      { "drop": null } ] } } }"#;

fn fixture_commands() -> Vec<serde_json::Value> {
    serde_json::from_str(&format!("[{FIXTURE_POLICY}]")).unwrap()
}

/// Render JSON with every object's keys in reverse order and no whitespace,
/// a spelling the policy file never uses.
fn reversed_keys(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_unstable_by(|(left, _), (right, _)| right.cmp(left));
            let entries: Vec<_> = entries
                .into_iter()
                .map(|(key, value)| format!("{}:{}", serde_json::json!(key), reversed_keys(value)))
                .collect();
            format!("{{{}}}", entries.join(","))
        }
        serde_json::Value::Array(items) => {
            let items: Vec<_> = items.iter().map(reversed_keys).collect();
            format!("[{}]", items.join(","))
        }
        scalar => scalar.to_string(),
    }
}

#[test]
fn hashes_ignore_formatting_and_key_order() {
    let installed = PartHash::sandbox_base(FIXTURE_POLICY).unwrap();
    let commands = fixture_commands();
    let compact_reversed = commands
        .iter()
        .map(reversed_keys)
        .collect::<Vec<_>>()
        .join(",");
    let pretty = commands
        .iter()
        .map(|command| serde_json::to_string_pretty(command).unwrap())
        .collect::<Vec<_>>()
        .join(",\n\n");
    for respelled in [compact_reversed, pretty] {
        assert_ne!(respelled, FIXTURE_POLICY);
        assert_eq!(PartHash::sandbox_base(&respelled).unwrap(), installed);
    }

    let script = super::super::guest_l2_ingress_rules_script();
    let respaced = script
        .lines()
        .map(|line| format!("\t{}  ", line.replace(' ', "   ")))
        .collect::<Vec<_>>()
        .join("\n\n");
    assert_eq!(
        PartHash::of_native_script(ConfigPart::L2Ingress, &respaced),
        ConfigPart::L2Ingress.builtin_hash().unwrap()
    );
}

#[test]
fn hashes_change_with_every_meaningful_rule_change() {
    type Mutation = dyn Fn(&mut Vec<serde_json::Value>);
    let base = fixture_commands();
    let installed = PartHash::sandbox_base(FIXTURE_POLICY).unwrap();
    let mutate = |change: &dyn Fn(&mut Vec<serde_json::Value>)| {
        let mut commands = base.clone();
        change(&mut commands);
        let policy = commands
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        PartHash::sandbox_base(&policy).unwrap()
    };
    let changes: [(&str, &Mutation); 7] = [
        ("rule order", &|commands| commands.swap(1, 2)),
        ("removed rule", &|commands| {
            commands.pop();
        }),
        ("matched port", &|commands| {
            commands[1]["add"]["rule"]["expr"][2]["match"]["right"] = 8443.into();
        }),
        ("set member order", &|commands| {
            commands[0]["add"]["rule"]["expr"][1]["match"]["right"]["set"] =
                serde_json::json!(["related", "established"]);
        }),
        ("verdict", &|commands| {
            commands[2]["add"]["rule"]["expr"][4] = serde_json::json!({ "accept": null });
        }),
        ("expression order", &|commands| {
            commands[2]["add"]["rule"]["expr"]
                .as_array_mut()
                .unwrap()
                .swap(2, 3);
        }),
        ("comment", &|commands| {
            commands[3]["add"]["rule"]["comment"] = "renamed".into();
        }),
    ];
    for (change, apply) in changes {
        assert_ne!(mutate(apply), installed, "{change} must change the hash");
    }

    // The base identity covers the entire transaction, not just its image policy.
    let rendered = super::super::namespace_rules_json(
        SLOT_VPEER,
        SLOT_VPEER_IP,
        crate::net::GUEST_IP,
        FIXTURE_POLICY,
    );
    let commands = serde_json::from_str::<super::super::NftRuleset>(&rendered)
        .unwrap()
        .nftables;
    assert_eq!(
        PartHash::of_commands(ConfigPart::SandboxBase, &commands),
        installed
    );
    for chain in [
        "pre_tproxy_guardrails",
        "guardrails",
        "postrouting",
        "prerouting",
    ] {
        let mut changed = commands.clone();
        let index = changed
            .iter()
            .position(|entry| entry["add"]["rule"]["chain"] == chain)
            .unwrap();
        changed.remove(index);
        assert_ne!(
            PartHash::of_commands(ConfigPart::SandboxBase, &changed),
            installed,
            "removing a {chain} rule must change the base hash"
        );
    }
    for (property, value) in [("prio", serde_json::json!(-174)), ("hook", "input".into())] {
        let mut changed = commands.clone();
        let chain = changed
            .iter_mut()
            .find(|entry| entry["add"]["chain"]["name"] == "pre_tproxy_guardrails")
            .unwrap();
        chain["add"]["chain"][property] = value;
        assert_ne!(
            PartHash::of_commands(ConfigPart::SandboxBase, &changed),
            installed,
            "chain {property} must change the hash"
        );
    }

    // The part is hashed too: identical rules are never confused across parts.
    assert_ne!(
        PartHash::of_commands(ConfigPart::Forward, &commands),
        installed
    );
    let builtin: std::collections::HashSet<_> = ConfigPart::ALL
        .into_iter()
        .filter_map(ConfigPart::builtin_hash)
        .collect();
    assert_eq!(builtin.len(), ConfigPart::ALL.len() - 1);
}

#[test]
fn malformed_packaged_policy_has_no_hash() {
    for policy in [
        "",
        "{ \"add\": ",
        "null",
        "[]",
        "{}",
        "true",
        "42",
        "\"rule\"",
        r#"{"unknown": {}}"#,
        r#"{"add": null}"#,
        r#"{"add": {"rule": null}}"#,
        r#"{"add": {"rule": {}}}"#,
        r#"{"add": {"rule": {"family": "ip", "table": "sandbox", "chain": "pre_tproxy_guardrails"}}}"#,
    ] {
        for fragment in [policy.to_owned(), format!("{FIXTURE_POLICY}, {policy}")] {
            assert!(
                matches!(
                    PartHash::sandbox_base(&fragment),
                    Err(FirewallError::PackagedPolicy(_))
                ),
                "invalid policy was not classified as a packaged-policy error: {fragment}"
            );
        }
    }
}

#[test]
fn markers_round_trip_and_parse_strictly() {
    let hash =
        PartHash::sandbox_base(super::super::packaged_pre_tproxy_guardrails_policy_for_test())
            .unwrap();
    for part in ConfigPart::ALL {
        assert_eq!(part.as_str().parse(), Ok(part));
        let hash = part.builtin_hash().unwrap_or(hash);
        let marker = hash.marker(part);
        // nft refuses rule comments longer than 128 bytes.
        assert!(marker.len() <= 128, "{marker}");
        assert_eq!(parse_marker(&marker), Ok((part, hash)));
    }

    let marker = hash.marker(ConfigPart::SandboxBase);
    let digest = marker.rsplit_once('.').unwrap().1;
    let malformed = [
        String::new(),
        "space-cfg:".into(),
        "space-cfg:sandbox_base=r1.".into(),
        "space-cfg:sandbox_base=1.".to_string() + digest,
        "space-cfg:sandbox_base=r+1.".to_string() + digest,
        "space-cfg:sandbox_base=r01.".to_string() + digest,
        "space-cfg:sandbox_base=r1.".to_string() + &digest.to_uppercase(),
        "space-cfg:sandbox_base=r1.".to_string() + &digest.chars().skip(1).collect::<String>(),
        marker.clone() + " ",
        marker.replace("space-cfg:", "space-cfg-"),
        "drop-spoofed-guest-source".into(),
    ];
    for comment in malformed {
        assert_eq!(
            parse_marker(&comment),
            Err(MarkerError::Malformed(comment.clone())),
            "{comment:?}"
        );
    }
    for comment in [
        marker.replace("space-cfg:sandbox_base=", "space-cfg:SANDBOX_BASE="),
        marker.replace("space-cfg:sandbox_base=", "space-cfg:snat="),
        marker.replace("space-cfg:sandbox_base=", "space-cfg:="),
    ] {
        assert_eq!(
            parse_marker(&comment),
            Err(MarkerError::UnknownPart(comment.clone()))
        );
    }
    assert_eq!(
        "snat".parse::<ConfigPart>(),
        Err(UnknownConfigPart("snat".into()))
    );
    for comment in [
        marker.replace("=r1.", "=r2."),
        "space-cfg:sandbox_base=r2.a-later-digest-format".into(),
    ] {
        assert_eq!(
            parse_marker(&comment),
            Err(MarkerError::UnsupportedRevision(comment.clone()))
        );
    }
}

#[test]
fn read_marker_requires_exactly_one_marker_for_its_part() {
    let part = ConfigPart::SandboxBase;
    let hash = PartHash::sandbox_base(FIXTURE_POLICY).unwrap();
    let marker = hash.marker(part);
    let other = ConfigPart::Forward.builtin_hash().unwrap();
    let read_comments = |comments: &[Option<&str>]| {
        let rules: Vec<_> = comments
            .iter()
            .map(|comment| {
                let mut rule = serde_json::json!({
                    "family": "ip", "table": "sandbox", "chain": "space_cfg_sandbox_base",
                    "expr": [{"continue": null}]
                });
                if let Some(comment) = comment {
                    rule["comment"] = (*comment).into();
                }
                serde_json::json!({"rule": rule})
            })
            .collect();
        read_marker(part, &serde_json::json!({"nftables": rules}))
    };

    assert_eq!(read_comments(&[]), Ok(None));
    assert_eq!(read_comments(&[Some(marker.as_str())]), Ok(Some(hash)));
    // A second marker is ambiguous even when it repeats the first.
    assert_eq!(
        read_comments(&[Some(marker.as_str()), Some(marker.as_str())]),
        Err(MarkerError::Duplicate(part))
    );
    assert_eq!(
        read_comments(&[Some(marker.as_str()), None]),
        Err(MarkerError::Uncommented(part))
    );
    assert_eq!(
        read_comments(&[Some(marker.as_str()), Some("space-cfg-note")]),
        Err(MarkerError::Malformed("space-cfg-note".into()))
    );
    assert_eq!(
        read_comments(&[Some(other.marker(ConfigPart::Forward).as_str())]),
        Err(MarkerError::WrongPart {
            expected: part,
            found: ConfigPart::Forward
        })
    );
}

/// `nft -j list ruleset` of a namespace after every full installation, from
/// each supported nftables version: `nft --version` in amazonlinux:2023
/// (1.0.4), ubuntu:24.04 (1.0.9) and debian:trixie (1.1.3). The rules must
/// match this binary's builders and the image's packaged policy, so
/// regenerate them after changing either: run the ignored
/// `firewall::tests::linux::print_stamped_namespace_listing` as root with
/// `SPACE_PRINT_NFT_FIXTURE=1` under each version, for example by running the test binary in a container with
/// `--cap-add NET_ADMIN --cap-add SYS_ADMIN`, and pipe its output through
/// `python3 -m json.tool --indent 2`.
fn stamped_listings() -> [serde_json::Value; 3] {
    [
        include_str!("../../fixtures/stamped_namespace_nft_1_0_4.json"),
        include_str!("../../fixtures/stamped_namespace_nft_1_0_9.json"),
        include_str!("../../fixtures/stamped_namespace_nft_1_1_3.json"),
    ]
    .map(|listing| serde_json::from_str(listing).unwrap())
}

#[test]
fn markers_read_back_from_each_supported_nft_version() {
    for listing in stamped_listings() {
        let version = &listing["nftables"][0]["metainfo"]["version"];
        for part in ConfigPart::ALL {
            // The captured digest belongs to the installation at capture
            // time, not today's builders. Live installation tests compare
            // kernel markers with the current builders' hashes.
            let prefix = format!("{MARKER_PREFIX}{part}=");
            let comment = listing["nftables"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|entry| entry["rule"]["comment"].as_str())
                .find(|comment| comment.starts_with(&prefix))
                .unwrap();
            let observed = read_marker(part, &listing).unwrap().unwrap();
            assert_eq!(observed.marker(part), comment, "nft {version} {part}");
        }
    }
    // Namespaces installed before markers existed read as unmarked.
    for legacy in [
        include_str!("../../fixtures/live_pre_tproxy_guardrails_chain.json"),
        include_str!("../../fixtures/live_inbound_reply_chain.json"),
        include_str!("../../fixtures/guest_l2_ingress_nft_1_0_4.json"),
    ] {
        let legacy: serde_json::Value = serde_json::from_str(legacy).unwrap();
        for part in ConfigPart::ALL {
            assert_eq!(read_marker(part, &legacy), Ok(None), "{part}");
        }
    }
}

#[test]
fn only_inert_well_formed_markers_are_read() {
    let [listing, ..] = stamped_listings();
    let find = |predicate: &dyn Fn(&serde_json::Value) -> bool| {
        listing["nftables"]
            .as_array()
            .unwrap()
            .iter()
            .position(predicate)
            .unwrap()
    };
    let base_marker = find(&|entry| entry["rule"]["chain"] == "space_cfg_sandbox_base");
    let base_chain = find(&|entry| entry["chain"]["name"] == "space_cfg_sandbox_base");
    let proxy_marker = find(&|entry| {
        entry["rule"]["table"] == "proxy"
            && entry["rule"]["comment"]
                .as_str()
                .is_some_and(|comment| comment.starts_with(MARKER_PREFIX))
    });
    let edit = |change: &dyn Fn(&mut Vec<serde_json::Value>)| {
        let mut changed = listing.clone();
        change(changed["nftables"].as_array_mut().unwrap());
        changed
    };

    // A marker that is not an inert continue is not a marker, in a dedicated
    // chain or in the shared proxy chain. Comment parsing, duplicates and
    // missing comments are covered above.
    for (part, index) in [
        (ConfigPart::SandboxBase, base_marker),
        (ConfigPart::Proxy, proxy_marker),
    ] {
        let verdict = edit(&|entries| {
            entries[index]["rule"]["expr"] = serde_json::json!([{ "accept": null }]);
        });
        assert_eq!(
            read_marker(part, &verdict),
            Err(MarkerError::NotInert(part))
        );
    }
    // A dedicated chain holds nothing but its marker, and nothing evaluates it.
    let extra_rule = edit(&|entries| {
        let mut extra = entries[base_marker].clone();
        extra["rule"]["comment"] = "drop-everything".into();
        entries.push(extra);
    });
    assert_eq!(
        read_marker(ConfigPart::SandboxBase, &extra_rule),
        Err(MarkerError::Malformed("drop-everything".into()))
    );
    // An earlier malformed comment takes precedence over a later bad expression.
    let mixed_errors = edit(&|entries| {
        entries[base_marker]["rule"]["comment"] = "space-cfg:invalid".into();
        let mut extra = entries[base_marker].clone();
        extra["rule"]["expr"] = serde_json::json!([{"accept": null}]);
        entries.push(extra);
    });
    assert_eq!(
        read_marker(ConfigPart::SandboxBase, &mixed_errors),
        Err(MarkerError::Malformed("space-cfg:invalid".into()))
    );
    let hooked = edit(&|entries| {
        entries[base_chain]["chain"]["hook"] = "prerouting".into();
    });
    assert_eq!(
        read_marker(ConfigPart::SandboxBase, &hooked),
        Err(MarkerError::HookedChain(ConfigPart::SandboxBase))
    );
    // Proxy rules without the marker prefix belong to the part, not its marker.
    let unmarked_proxy = edit(&|entries| {
        entries.remove(proxy_marker);
    });
    assert_eq!(read_marker(ConfigPart::Proxy, &unmarked_proxy), Ok(None));
    // A marker at another part's location is evidence of neither part.
    let moved = edit(&|entries| {
        let forward_marker = entries
            .iter()
            .position(|entry| entry["rule"]["chain"] == "space_cfg_forward")
            .unwrap();
        entries[forward_marker]["rule"]["comment"] =
            entries[base_marker]["rule"]["comment"].clone();
        entries.remove(base_marker);
    });
    assert_eq!(read_marker(ConfigPart::SandboxBase, &moved), Ok(None));
    assert_eq!(
        read_marker(ConfigPart::Forward, &moved),
        Err(MarkerError::WrongPart {
            expected: ConfigPart::Forward,
            found: ConfigPart::SandboxBase
        })
    );
}
