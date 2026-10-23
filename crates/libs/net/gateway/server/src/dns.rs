use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use pplx_space_metrics::gateway::{
    DnsAttributionCacheEvictionReason, DnsAttributionCacheLookupKind,
    DnsAttributionCacheUpdateResult,
};
use pplx_space_net_common::NetworkPolicyV1;
use tracing::debug;

const MAX_DNS_TTL: Duration = Duration::from_secs(300);
const MIN_DNS_TTL: Duration = Duration::from_secs(60);
// Exact-name binding makes this a correctness bound as well as a memory bound:
// one shared edge address must retain a practical set of allowed tenant names.
const MAX_ENTRIES_PER_IP: usize = 32;
const MAX_TOTAL_ENTRIES: usize = 4096;

/// Bounded cache of DNS-learned `ip -> allowed domain` mappings.
///
/// An entry is created only for a domain allow resolving to an IP the policy
/// allows, and only from answers the gateway obtained itself through the
/// reserved resolvers: replies on guest-addressed UDP/53 flows are guest-chosen
/// data and never reach this cache. Reads re-check the installed policy so a
/// stale entry cannot grant access in the interval before a policy-install
/// sweep removes it.
#[derive(Clone)]
pub struct DnsAllowCache {
    inner: Arc<ArcSwap<HashMap<IpAddr, Vec<DnsAllowEntry>>>>,
    /// Serializes load-clone-mutate-store writers. Reads stay lock-free via
    /// `ArcSwap::load`, but ArcSwap does not make read-modify-write sequences
    /// atomic by itself.
    write_lock: Arc<Mutex<()>>,
}

#[derive(Clone)]
struct DnsAllowEntry {
    domain: String,
    expires_at: Instant,
}

impl Default for DnsAllowCache {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            write_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl DnsAllowCache {
    pub fn allows_ip(&self, ip: IpAddr, policy: &NetworkPolicyV1) -> bool {
        let now = Instant::now();
        let hit = self.inner.load().get(&ip).is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.expires_at > now
                    && policy.allows_domain(entry.domain.as_str())
                    && entry_still_relevant(policy, ip, entry.domain.as_str())
            })
        });
        pplx_space_metrics::gateway::dns_attribution_cache_lookup(
            DnsAttributionCacheLookupKind::IpAllow,
            hit,
        );
        hit
    }

    /// Whether a live DNS response from a reserved resolver bound this exact
    /// `host` to `ip`.
    ///
    /// An IP-wide mapping is not enough: CDN edge addresses are shared by
    /// unrelated tenants whose services are selected by SNI or HTTP `Host`.
    pub fn contains_host_binding(&self, ip: IpAddr, host: &str) -> bool {
        let host = host.trim_end_matches('.');
        let now = Instant::now();
        let hit = self.inner.load().get(&ip).is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry.expires_at > now && entry.domain.eq_ignore_ascii_case(host))
        });
        pplx_space_metrics::gateway::dns_attribution_cache_lookup(
            DnsAttributionCacheLookupKind::HostBinding,
            hit,
        );
        hit
    }

    pub fn prune_expired(&self) {
        let _write_guard = self
            .write_lock
            .lock()
            .expect("DNS allow-cache lock poisoned");
        let now = Instant::now();
        let mut inner = self.inner.load().as_ref().clone();
        let before = total_entries(&inner);
        inner.retain(|_, entries| {
            entries.retain(|entry| entry.expires_at > now);
            !entries.is_empty()
        });
        let after = total_entries(&inner);
        if after != before {
            self.inner.store(Arc::new(inner));
        }
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::Expired,
            before - after,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_entries(after);
    }

    /// Drop entries the freshly installed `policy` no longer justifies.
    pub fn retain_relevant_to_policy(&self, policy: &NetworkPolicyV1) {
        let _write_guard = self
            .write_lock
            .lock()
            .expect("DNS allow-cache lock poisoned");
        let now = Instant::now();
        let mut inner = self.inner.load().as_ref().clone();
        let mut expired = 0;
        let mut policy_pruned = 0;
        inner.retain(|ip, entries| {
            entries.retain(|entry| {
                if entry.expires_at <= now {
                    expired += 1;
                    false
                } else if !entry_still_relevant(policy, *ip, &entry.domain) {
                    policy_pruned += 1;
                    false
                } else {
                    true
                }
            });
            !entries.is_empty()
        });
        let entries = total_entries(&inner);
        if expired + policy_pruned > 0 {
            self.inner.store(Arc::new(inner));
        }
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::Expired,
            expired,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::Policy,
            policy_pruned,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_entries(entries);
    }

    /// Returns whether the response supplied cacheable attribution evidence.
    pub fn observe_response(&self, packet: &[u8], policy: &NetworkPolicyV1) -> bool {
        let Some(response) = DnsResponse::parse(packet) else {
            return false;
        };
        if response.answers.is_empty() {
            return false;
        }

        // Build the allow graph as the set of names *reachable from the
        // guest's own DNS questions* via CNAME edges. Each reachable name maps
        // to the set of allowed domains associated with it. This is scoped
        // to the queried names on purpose: an unrelated answer record bundled
        // into the response must not create allow evidence for that name.
        //
        // A reachable name attributes to itself when it is policy-relevant, and
        // carries its allowed_names down its CNAME edge to the target. Seeding
        // the target's own name too preserves an independently allowed target.
        //
        // Index CNAME edges by owner so the traversal follows the full chain
        // regardless of answer order.
        let cname_targets: BTreeMap<&str, &str> = response
            .answers
            .iter()
            .filter_map(|answer| {
                answer
                    .cname
                    .as_deref()
                    .map(|cname| (answer.name.as_str(), cname))
            })
            .collect();

        let allowed_names = reachable_allowed_names(&response.questions, &cname_targets, policy);

        let now = Instant::now();
        let mut records = Vec::new();

        for answer in &response.answers {
            if let Some(domains) = allowed_names.get(answer.name.as_str())
                && let Some(ip) = answer.ip
                && answer_is_cacheable(policy, ip)
            {
                let ttl =
                    Duration::from_secs(u64::from(answer.ttl)).clamp(MIN_DNS_TTL, MAX_DNS_TTL);
                for domain in domains.iter() {
                    records.push((ip, normalize_dns_name(domain), now + ttl));
                }
            }
        }

        if records.is_empty() {
            return false;
        }

        let _write_guard = self
            .write_lock
            .lock()
            .expect("DNS allow-cache lock poisoned");
        let mut inner = self.inner.load().as_ref().clone();
        let before_expiry_prune = total_entries(&inner);
        inner.retain(|_, entries| {
            entries.retain(|entry| entry.expires_at > now);
            !entries.is_empty()
        });
        let expired = before_expiry_prune - total_entries(&inner);
        let mut inserted = 0;
        let mut refreshed = 0;
        let mut per_ip_capacity_evicted = 0;
        for (ip, domain, expires_at) in records {
            // The domain (a guest-controlled DNS name) and the resolved IP are
            // not logged raw: a sandbox can encode arbitrary data in a valid
            // hostname and exfiltrate it to the log sink, and the address is
            // sensitive connection metadata. Log bounded, non-sensitive fields.
            debug!(
                domain_len = domain.len(),
                address_family = if ip.is_ipv6() { "ipv6" } else { "ipv4" },
                "cached DNS domain allow"
            );
            let entries = inner.entry(ip).or_default();
            // Dedup by domain: a repeated observation refreshes the existing
            // entry's TTL rather than appending a duplicate.
            if let Some(existing) = entries.iter_mut().find(|entry| entry.domain == domain) {
                existing.expires_at = existing.expires_at.max(expires_at);
                refreshed += 1;
            } else {
                entries.push(DnsAllowEntry { domain, expires_at });
                inserted += 1;
            }
            // Keep the longest-lived entries when the per-IP cap is reached.
            entries.sort_by_key(|entry| std::cmp::Reverse(entry.expires_at));
            let before = entries.len();
            entries.truncate(MAX_ENTRIES_PER_IP);
            per_ip_capacity_evicted += before - entries.len();
        }
        let global_capacity_evicted = trim_global_cache(&mut inner);
        let entries = total_entries(&inner);
        self.inner.store(Arc::new(inner));
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::Expired,
            expired,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_update(
            DnsAttributionCacheUpdateResult::Insert,
            inserted,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_update(
            DnsAttributionCacheUpdateResult::Refresh,
            refreshed,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::PerIpCapacity,
            per_ip_capacity_evicted,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_eviction(
            DnsAttributionCacheEvictionReason::GlobalCapacity,
            global_capacity_evicted,
        );
        pplx_space_metrics::gateway::dns_attribution_cache_entries(entries);
        true
    }
}

/// Allowed names that reached one DNS owner through the response's
/// question-rooted CNAME graph.
///
/// The set is bounded so propagation remains finite even for cyclic CNAME
/// graphs and no guest-controlled string is cloned at each hop.
#[derive(Clone, Default)]
struct ReachableAllowedNames<'a> {
    names: Vec<&'a str>,
}

impl<'a> ReachableAllowedNames<'a> {
    fn insert_allowed(&mut self, policy: &NetworkPolicyV1, name: &'a str) -> bool {
        if !domain_evidence_relevant(policy, name) {
            return false;
        }
        if self.names.len() >= MAX_ENTRIES_PER_IP || self.names.contains(&name) {
            return false;
        }
        self.names.push(name);
        true
    }

    fn merge(&mut self, inherited: &Self) -> bool {
        let mut changed = false;
        for &name in &inherited.names {
            if self.names.len() >= MAX_ENTRIES_PER_IP {
                break;
            }
            if !self.names.contains(&name) {
                self.names.push(name);
                changed = true;
            }
        }
        changed
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        self.names.iter().copied()
    }
}

/// Propagate allowed names through the question-rooted CNAME graph.
///
/// A work queue computes a bounded monotonic fixed point. Each owner has one
/// outgoing CNAME edge and a capped allow set, so even a cyclic or maximally
/// packed DNS datagram performs bounded work per parsed name.
fn reachable_allowed_names<'a>(
    questions: &'a [String],
    cname_targets: &BTreeMap<&'a str, &'a str>,
    policy: &NetworkPolicyV1,
) -> BTreeMap<&'a str, ReachableAllowedNames<'a>> {
    let mut allowed_names: BTreeMap<&str, ReachableAllowedNames<'_>> = BTreeMap::new();
    let mut pending = VecDeque::new();

    for question in questions {
        let question = question.as_str();
        let is_new = !allowed_names.contains_key(question);
        let changed = allowed_names
            .entry(question)
            .or_default()
            .insert_allowed(policy, question);
        if is_new || changed {
            pending.push_back(question);
        }
    }

    while let Some(owner) = pending.pop_front() {
        let Some(&target) = cname_targets.get(owner) else {
            continue;
        };
        // The sets contain borrowed names and are capped at a small constant;
        // cloning them releases the map borrow before the target is mutated
        // without cloning guest-controlled strings at every CNAME hop.
        let inherited = allowed_names
            .get(owner)
            .expect("queued DNS owner must have allowed-name state")
            .clone();
        let is_new = !allowed_names.contains_key(target);
        let target_allowed_names = allowed_names.entry(target).or_default();
        let changed = target_allowed_names.insert_allowed(policy, target)
            | target_allowed_names.merge(&inherited);
        if is_new || changed {
            pending.push_back(target);
        }
    }

    allowed_names
}

fn normalize_dns_name(name: &str) -> String {
    // Keep the stored side canonical too; exact-name checks must not depend on
    // which syntactic form the DNS parser produced.
    name.trim_end_matches('.').to_ascii_lowercase()
}

/// Never cache an entry for an IP the policy denies, so cache membership stays
/// evidence that cannot override address denies.
fn answer_is_cacheable(policy: &NetworkPolicyV1, ip: IpAddr) -> bool {
    policy
        .egress_decision_for_ip_with_dns_allow(ip, true)
        .is_allowed()
}

/// Exemption evidence is retained independently of DNS-derived egress grants.
fn domain_evidence_relevant(policy: &NetworkPolicyV1, name: &str) -> bool {
    let Ok(domain) = name.parse() else {
        return false;
    };
    !policy.denies_domain(&domain)
        && (policy.matches_domain_allow_rule(&domain)
            || policy
                .tls_interception_exemptions
                .iter()
                .any(|exemption| exemption.destination.matches_domain_name(&domain)))
}

fn entry_still_relevant(policy: &NetworkPolicyV1, ip: IpAddr, domain: &str) -> bool {
    policy
        .egress_decision_for_ip_with_dns_allow(ip, true)
        .is_allowed()
        && domain_evidence_relevant(policy, domain)
}

struct DnsResponse {
    questions: Vec<String>,
    answers: Vec<DnsAnswer>,
}

struct DnsAnswer {
    name: String,
    ip: Option<IpAddr>,
    cname: Option<String>,
    ttl: u32,
}

impl DnsResponse {
    fn parse(packet: &[u8]) -> Option<Self> {
        if packet.len() < 12 {
            return None;
        }
        let flags = read_u16(packet, 2)?;
        if flags & 0x8000 == 0 {
            return None;
        }
        if flags & 0x0200 != 0 {
            return None;
        }
        if flags & 0x000f != 0 {
            return None;
        }

        let qdcount = usize::from(read_u16(packet, 4)?);
        let ancount = usize::from(read_u16(packet, 6)?);
        let mut offset = 12;
        let mut questions = Vec::new();

        for _ in 0..qdcount {
            let name = read_name(packet, &mut offset, 0)?;
            if offset + 4 > packet.len() {
                return None;
            }
            offset += 4;
            questions.push(name);
        }

        let mut answers = Vec::new();
        for _ in 0..ancount {
            let name = read_name(packet, &mut offset, 0)?;
            if offset + 10 > packet.len() {
                return None;
            }
            let record_type = read_u16(packet, offset)?;
            let class = read_u16(packet, offset + 2)?;
            let ttl = read_u32(packet, offset + 4)?;
            let rdlen = usize::from(read_u16(packet, offset + 8)?);
            offset += 10;
            if offset + rdlen > packet.len() {
                return None;
            }
            let rdata_offset = offset;
            let rdata_end = offset + rdlen;
            if class == 1 && record_type == 1 && rdlen == 4 {
                answers.push(DnsAnswer {
                    name,
                    ip: Some(IpAddr::V4(Ipv4Addr::from(read_array::<4>(packet, offset)?))),
                    cname: None,
                    ttl,
                });
            } else if class == 1 && record_type == 28 && rdlen == 16 {
                answers.push(DnsAnswer {
                    name,
                    ip: Some(IpAddr::V6(Ipv6Addr::from(read_array::<16>(
                        packet, offset,
                    )?))),
                    cname: None,
                    ttl,
                });
            } else if class == 1 && record_type == 5 {
                let mut cname_offset = rdata_offset;
                let cname = read_name(packet, &mut cname_offset, 0)?;
                if cname_offset > rdata_end {
                    return None;
                }
                answers.push(DnsAnswer {
                    name,
                    ip: None,
                    cname: Some(cname),
                    ttl,
                });
            }
            offset = rdata_end;
        }

        Some(Self { questions, answers })
    }
}

fn read_array<const N: usize>(packet: &[u8], offset: usize) -> Option<[u8; N]> {
    packet.get(offset..offset.checked_add(N)?)?.try_into().ok()
}

fn read_u16(packet: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_be_bytes(read_array::<2>(packet, offset)?))
}

fn read_u32(packet: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(read_array::<4>(packet, offset)?))
}

fn read_name(packet: &[u8], offset: &mut usize, depth: u8) -> Option<String> {
    if depth > 8 {
        return None;
    }

    let mut labels = Vec::new();
    loop {
        let len = *packet.get(*offset)?;
        if len & 0xc0 == 0xc0 {
            let pointer = usize::from(u16::from_be_bytes([
                len & 0x3f,
                *packet.get((*offset).checked_add(1)?)?,
            ]));
            *offset += 2;
            let mut pointer_offset = pointer;
            labels.push(read_name(packet, &mut pointer_offset, depth + 1)?);
            break;
        }
        *offset += 1;
        if len == 0 {
            break;
        }
        let len = usize::from(len);
        if len > 63 || *offset + len > packet.len() {
            return None;
        }
        let label = packet.get(*offset..(*offset).checked_add(len)?)?;
        labels.push(String::from_utf8_lossy(label).to_ascii_lowercase());
        *offset += len;
    }

    Some(labels.join("."))
}

/// Evict the soonest-to-expire entries until the global cap is satisfied.
fn trim_global_cache(inner: &mut HashMap<IpAddr, Vec<DnsAllowEntry>>) -> usize {
    let mut total = total_entries(inner);
    if total <= MAX_TOTAL_ENTRIES {
        return 0;
    }
    let before = total;

    let mut candidates: Vec<(Instant, IpAddr, String)> = inner
        .iter()
        .flat_map(|(ip, entries)| {
            entries
                .iter()
                .map(move |entry| (entry.expires_at, *ip, entry.domain.clone()))
        })
        .collect();
    candidates.sort_by_key(|candidate| candidate.0);

    for (_, ip, domain) in candidates {
        if total <= MAX_TOTAL_ENTRIES {
            break;
        }
        if let Some(entries) = inner.get_mut(&ip)
            && let Some(pos) = entries.iter().position(|entry| entry.domain == domain)
        {
            entries.remove(pos);
            total -= 1;
        }
    }
    inner.retain(|_, entries| !entries.is_empty());
    before - total
}

fn total_entries(inner: &HashMap<IpAddr, Vec<DnsAllowEntry>>) -> usize {
    inner.values().map(Vec::len).sum()
}

#[cfg(test)]
pub(crate) mod test_fixtures {
    pub(crate) fn expire_cache(cache: &super::DnsAllowCache) {
        let _guard = cache.write_lock.lock().unwrap();
        let mut entries = cache.inner.load().as_ref().clone();
        let expired = std::time::Instant::now();
        for entry in entries.values_mut().flatten() {
            entry.expires_at = expired;
        }
        cache.inner.store(std::sync::Arc::new(entries));
    }

    /// Minimal successful DNS response with one A question and answer.
    pub(crate) fn a_response(name: &str, ip: [u8; 4], ttl: u32) -> Vec<u8> {
        let mut out = vec![
            0x12, 0x34, 0x81, 0x80, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
        ];
        write_name(&mut out, name);
        out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
        write_name(&mut out, name);
        out.extend_from_slice(&[0x00, 0x01, 0x00, 0x01]);
        out.extend_from_slice(&ttl.to_be_bytes());
        out.extend_from_slice(&[0x00, 0x04]);
        out.extend_from_slice(&ip);
        out
    }

    fn write_name(out: &mut Vec<u8>, name: &str) {
        for label in name.split('.') {
            out.push(label.len() as u8);
            out.extend_from_slice(label.as_bytes());
        }
        out.push(0);
    }
}

#[cfg(test)]
mod tests;
