//! Stable versions of four firewall installation groups in a slot's namespace.
//!
//! Only a full installation records a [`ConfigPart`]'s version. Incremental
//! repairs leave its marker unchanged. Host and per-port tables are excluded.
//!
//! Hash the installation builders with fixed slot inputs and the packaged
//! policy included in `sandbox_base`. Normalize JSON keys and whitespace, keeping
//! rule order and values. [`HASH_REVISION`] versions this format; markers use
//! `space-cfg:<part>=r<revision>.<digest>`.
//!
//! Markers commit atomically with their installation. They record its identity,
//! not proof that the rules have remained unchanged.

use serde::de::Error as _;
use sha2::Digest;

use super::{FirewallError, FirewallResult, PackagedPolicyError};

/// The marker comment prefix: `space-cfg:<part>=r<revision>.<digest>`.
pub const MARKER_PREFIX: &str = "space-cfg:";

/// The revision of the hash computation below. Bump it when a change to the
/// canonical form or digest would give unchanged rules a different hash.
pub const HASH_REVISION: u32 = 1;

const DIGEST_BYTES: usize = 16;

/// Stand-ins for the slot-specific inputs when rendering a part's template.
const SLOT_VPEER: &str = "{vpeer}";
const SLOT_VPEER_IP: &str = "{vpeer_ip}";
const SLOT_GATEWAY_UID: u32 = 0;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ConfigPart {
    /// `sandbox_l2/ingress`: setup/reset and startup containment flush and
    /// reinstall it. No incremental writers.
    L2Ingress,
    /// One sandbox table transaction: pre-TPROXY guards, packaged policy,
    /// egress guardrails and NAT. DHCP exceptions are incremental.
    SandboxBase,
    /// `sandbox/forward`: `ensure_namespace_forward_drop_rules` installs it
    /// only when creating the chain. Later drop/port/reply edits are incremental.
    Forward,
    /// `proxy/prerouting` and `divert`: gateway launch flushes both chains;
    /// reply-path repair is incremental. The hash covers both DHCP variants.
    Proxy,
}

impl ConfigPart {
    pub const ALL: [Self; 4] = [
        Self::L2Ingress,
        Self::SandboxBase,
        Self::Forward,
        Self::Proxy,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L2Ingress => "l2_ingress",
            Self::SandboxBase => "sandbox_base",
            Self::Forward => "forward",
            Self::Proxy => "proxy",
        }
    }

    /// This binary's hash of a group without image-provided rules.
    /// Sandbox base requires the image policy; use [`PartHash::sandbox_base`].
    pub fn builtin_hash(self) -> Option<PartHash> {
        let commands = match self {
            Self::L2Ingress => {
                return Some(PartHash::of_native_script(
                    self,
                    &super::guest_l2_ingress_rules_script(),
                ));
            }
            Self::SandboxBase => return None,
            Self::Forward => {
                builder_commands(&super::forward_install_commands_json(SLOT_VPEER).join(",\n"))
            }
            Self::Proxy => [false, true]
                .into_iter()
                .flat_map(|dhcp| {
                    let rendered = super::netgtw_tproxy_rules_json(SLOT_GATEWAY_UID, dhcp);
                    serde_json::from_str::<super::NftRuleset>(&rendered)
                        .expect("the TPROXY builder renders an nftables JSON ruleset")
                        .nftables
                })
                .collect(),
        };
        Some(PartHash::of_commands(self, &commands))
    }
}

impl std::fmt::Display for ConfigPart {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown firewall configuration part {0:?}")]
pub struct UnknownConfigPart(String);

impl std::str::FromStr for ConfigPart {
    type Err = UnknownConfigPart;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|part| part.as_str() == value)
            .ok_or_else(|| UnknownConfigPart(value.to_string()))
    }
}

/// One part's rules as a full installation writes them, under
/// [`HASH_REVISION`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PartHash([u8; DIGEST_BYTES]);

impl PartHash {
    /// Hash the complete sandbox base transaction, including the image policy
    /// (comma-separated nft JSON add-rule commands), with fixed slot inputs.
    pub fn sandbox_base(policy: &str) -> FirewallResult<Self> {
        let packaged: Vec<serde_json::Value> =
            serde_json::from_str(&format!("[{policy}]")).map_err(PackagedPolicyError)?;
        // Check the command envelope before hashing arbitrary JSON as policy.
        // nft remains responsible for validating rule expression semantics.
        if packaged.is_empty()
            || packaged.iter().any(|command| {
                let Some(rule) = command.pointer("/add/rule") else {
                    return true;
                };
                command.as_object().is_none_or(|object| object.len() != 1)
                    || command["add"]
                        .as_object()
                        .is_none_or(|object| object.len() != 1)
                    || !["family", "table", "chain"]
                        .into_iter()
                        .all(|field| rule.get(field).is_some_and(serde_json::Value::is_string))
                    || !rule["expr"].is_array()
            })
        {
            return Err(PackagedPolicyError(serde_json::Error::custom(
                "expected nonempty add-rule policy with family, table, chain and expr",
            ))
            .into());
        }
        let rendered =
            super::namespace_rules_json(SLOT_VPEER, SLOT_VPEER_IP, crate::net::GUEST_IP, policy);
        let ruleset: super::NftRuleset =
            serde_json::from_str(&rendered).map_err(FirewallError::RenderedRuleset)?;
        Ok(Self::of_commands(
            ConfigPart::SandboxBase,
            &ruleset.nftables,
        ))
    }

    fn of_commands(part: ConfigPart, commands: &[serde_json::Value]) -> Self {
        let mut canonical = String::new();
        for mut command in commands.iter().cloned() {
            // Explicit sorting also works when serde_json enables preserve_order.
            command.sort_all_objects();
            canonical.push_str(&command.to_string());
            canonical.push('\n');
        }
        Self::digest(part, &canonical)
    }

    fn of_native_script(part: ConfigPart, script: &str) -> Self {
        let canonical = script
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        Self::digest(part, &canonical)
    }

    fn digest(part: ConfigPart, canonical: &str) -> Self {
        let mut hasher = sha2::Sha256::new();
        hasher.update(format!("space-cfg r{HASH_REVISION}\0{part}\0"));
        hasher.update(canonical);
        let digest = hasher.finalize();
        Self(
            *digest
                .first_chunk::<DIGEST_BYTES>()
                .expect("a SHA-256 digest is longer than its truncation"),
        )
    }

    /// The marker comment recording that `part` was installed with this hash.
    pub fn marker(self, part: ConfigPart) -> String {
        format!("{MARKER_PREFIX}{part}={self}")
    }
}

impl std::fmt::Display for PartHash {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "r{HASH_REVISION}.{}", hex::encode(self.0))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum MarkerError {
    #[error("marker {0:?} is malformed")]
    Malformed(String),
    #[error("a rule at the {0} marker location has no comment")]
    Uncommented(ConfigPart),
    #[error("a rule at the {0} marker location is not an inert continue")]
    NotInert(ConfigPart),
    #[error("the {0} marker chain is a base chain")]
    HookedChain(ConfigPart),
    #[error("marker {0:?} names an unknown configuration part")]
    UnknownPart(String),
    #[error("marker {0:?} uses an unsupported hash revision")]
    UnsupportedRevision(String),
    #[error("the {expected} marker location holds a {found} marker")]
    WrongPart {
        expected: ConfigPart,
        found: ConfigPart,
    },
    #[error("the {0} marker location holds more than one marker")]
    Duplicate(ConfigPart),
}

/// Parse a marker comment strictly: exactly `space-cfg:<part>=r1.<digest>`,
/// with a known part and 32 lowercase hex digits.
fn parse_marker(comment: &str) -> Result<(ConfigPart, PartHash), MarkerError> {
    let malformed = || MarkerError::Malformed(comment.to_string());
    let (part, hash) = comment
        .strip_prefix(MARKER_PREFIX)
        .and_then(|body| body.split_once('='))
        .ok_or_else(malformed)?;
    let part: ConfigPart = part
        .parse()
        .map_err(|_| MarkerError::UnknownPart(comment.into()))?;
    let (revision, digest) = hash
        .strip_prefix('r')
        .and_then(|hash| hash.split_once('.'))
        .ok_or_else(malformed)?;
    // A revision is a decimal number written without leading zeros, so each
    // revision has exactly one spelling.
    let canonical_number = !revision.is_empty()
        && revision.bytes().all(|byte| byte.is_ascii_digit())
        && (revision == "0" || !revision.starts_with('0'));
    if !canonical_number {
        return Err(malformed());
    }
    if revision.parse::<u32>() != Ok(HASH_REVISION) {
        return Err(MarkerError::UnsupportedRevision(comment.into()));
    }
    let lowercase_hex = digest
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !lowercase_hex || digest.len() != DIGEST_BYTES * 2 {
        return Err(malformed());
    }
    let mut bytes = [0; DIGEST_BYTES];
    hex::decode_to_slice(digest, &mut bytes).map_err(|_| malformed())?;
    Ok((part, PartHash(bytes)))
}

/// Where a part's marker lives. A dedicated chain holds nothing else; in a
/// shared chain, only rules with the marker prefix are markers.
///
/// Sandbox and L2 markers use unhooked, unreferenced chains, keeping them out
/// of rule validators. Recreating their table removes them; L2 containment
/// flushes and rewrites its marker alongside the policy. The proxy marker
/// follows the readiness rule's `return` in `prerouting`, which every gateway
/// launch flushes, including launches by older binaries.
///
/// Two pre-marker rollback paths can leave a marker behind:
/// - Reset appends to a sandbox table that teardown failed to delete. Its
///   surviving marker describes only the original installation; appended rules
///   can affect fall-through traffic. A marker-aware reset uses `create`,
///   rejects the leftover table, and rebuilds the namespace instead.
/// - Startup containment rewrites L2 rules without clearing their marker.
///   On roll forward, containment reinstalls both the rules and their marker.
struct MarkerLocation {
    family: &'static str,
    table: &'static str,
    chain: &'static str,
    dedicated: bool,
}

impl ConfigPart {
    const fn marker_location(self) -> MarkerLocation {
        let (family, table, chain, dedicated) = match self {
            Self::L2Ingress => (
                "netdev",
                super::SANDBOX_L2_TABLE,
                "space_cfg_l2_ingress",
                true,
            ),
            Self::SandboxBase => ("ip", super::SANDBOX_TABLE, "space_cfg_sandbox_base", true),
            Self::Forward => ("ip", super::SANDBOX_TABLE, "space_cfg_forward", true),
            Self::Proxy => ("ip", super::PROXY_TABLE, "prerouting", false),
        };
        MarkerLocation {
            family,
            table,
            chain,
            dedicated,
        }
    }
}

/// The nft JSON commands that record `hash` for `part`. Add them to the
/// transaction that installs the part, after the rules.
pub(super) fn marker_commands(part: ConfigPart, hash: PartHash) -> Vec<serde_json::Value> {
    let location = part.marker_location();
    let rule = serde_json::json!({ "add": { "rule": {
        "family": location.family, "table": location.table, "chain": location.chain,
        "comment": hash.marker(part),
        "expr": [{ "continue": null }]
    } } });
    if !location.dedicated {
        return vec![rule];
    }
    let chain = serde_json::json!({
        "family": location.family, "table": location.table, "name": location.chain
    });
    vec![
        serde_json::json!({ "add": { "chain": chain } }),
        serde_json::json!({ "flush": { "chain": chain } }),
        rule,
    ]
}

/// [`marker_commands`] for this binary's `l2_ingress` hash in nft's native
/// syntax, which the L2 policy transaction uses.
pub(super) fn l2_ingress_marker_script() -> String {
    let part = ConfigPart::L2Ingress;
    let hash = part
        .builtin_hash()
        .expect("l2_ingress is hashed from this binary's builder");
    let MarkerLocation {
        family,
        table,
        chain,
        ..
    } = part.marker_location();
    format!(
        "add chain {family} {table} {chain}
flush chain {family} {table} {chain}
add rule {family} {table} {chain} continue comment \"{}\"
",
        hash.marker(part)
    )
}

/// The hash a namespace's markers record for `part`, read from an `nft -j`
/// listing of the ruleset or of the marker's chain. `None` means no marker,
/// as in a namespace installed before markers existed or a part not
/// installed. A marker must be exactly an inert `continue` rule. Dedicated
/// marker chains must be unhooked and contain only that marker; shared chains
/// may contain other rules, but any rule with the marker prefix must have the
/// marker's form. After checking chain metadata, report the first invalid
/// rule in listing order.
pub fn read_marker(
    part: ConfigPart,
    listing: &serde_json::Value,
) -> Result<Option<PartHash>, MarkerError> {
    let location = part.marker_location();
    let at_location = |object: &serde_json::Value, name_key: &str| {
        object["family"] == location.family
            && object["table"] == location.table
            && object[name_key] == location.chain
    };
    let entries = listing["nftables"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if location.dedicated
        && entries
            .iter()
            .filter_map(|entry| entry.get("chain"))
            .any(|chain| at_location(chain, "name") && chain.get("hook").is_some())
    {
        return Err(MarkerError::HookedChain(part));
    }
    let mut installed = None;
    for rule in entries
        .iter()
        .filter_map(|entry| entry.get("rule"))
        .filter(|rule| at_location(rule, "chain"))
    {
        let comment = rule["comment"].as_str();
        let marked = comment.is_some_and(|comment| comment.starts_with(MARKER_PREFIX));
        if !location.dedicated && !marked {
            continue;
        }
        if rule["expr"] != serde_json::json!([{ "continue": null }]) {
            return Err(MarkerError::NotInert(part));
        }
        let comment = comment.ok_or(MarkerError::Uncommented(part))?;
        let (found, hash) = parse_marker(comment)?;
        if found != part {
            return Err(MarkerError::WrongPart {
                expected: part,
                found,
            });
        }
        if installed.replace(hash).is_some() {
            return Err(MarkerError::Duplicate(part));
        }
    }
    Ok(installed)
}

fn builder_commands(fragment: &str) -> Vec<serde_json::Value> {
    serde_json::from_str(&format!("[{fragment}]"))
        .expect("namespace rule builders render comma-separated JSON commands")
}

#[cfg(test)]
mod tests;
