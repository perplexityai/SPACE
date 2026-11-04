//! Which version of its nftables rules each slot's namespace carries,
//! compared with the rules this node-manager installs.
//!
//! A new configuration arrives only with a node-manager restart, so startup
//! classifies every slot once, from the stamps containment read. After that
//! only node-manager's own installs change a namespace's rules, and their
//! outcomes keep the state without listing any namespace again. Rule edits
//! made outside node-manager that leave a stamp in place go unnoticed, and a
//! slot whose namespace is torn down keeps its last state until its next
//! install.

use dashmap::DashMap;

use crate::firewall::{NamespaceStamps, Part, PartState, PartVersion, PerPart, StampReading};

/// A slot's rules, part by part. The proxy part is `None` while its rules
/// are absent, as they are whenever net-gateway is not running; an absent
/// proxy is never unknown. Proxy rules whose stamp is missing or unusable
/// are not absent: they read unknown.
pub(crate) type SlotRules = PerPart<Option<PartState>>;

#[derive(Default)]
pub(crate) struct RuleVersions {
    slots: DashMap<u32, SlotRules>,
}

impl RuleVersions {
    /// Classify each slot from what startup read in its namespace: its
    /// stamps, or `None` when startup did not visit it. `current` is this
    /// binary's versions, or `None` when they could not be computed, in
    /// which case every part reads unknown.
    pub(crate) fn at_startup(
        slots: impl IntoIterator<Item = (u32, Option<NamespaceStamps>)>,
        current: Option<&PerPart<PartVersion>>,
    ) -> Self {
        let slots = slots
            .into_iter()
            .map(|(slot, stamps)| (slot, classify(stamps.as_ref(), current)))
            .collect();
        Self { slots }
    }

    /// Record a create or reset of a slot's namespace rules. Both remove
    /// the proxy rules and rebuild every other part.
    pub(crate) fn base_installed(&self, slot: u32, succeeded: bool) {
        let rules = PerPart::from_fn(|part| match (part, succeeded) {
            (Part::Proxy, true) => None,
            (_, true) => Some(PartState::Current),
            (_, false) => Some(PartState::Unknown),
        });
        self.slots.insert(slot, rules);
    }

    /// Record a net-gateway launch, which rebuilds only the proxy rules.
    pub(crate) fn proxy_installed(&self, slot: u32, succeeded: bool) {
        let proxy = if succeeded {
            PartState::Current
        } else {
            PartState::Unknown
        };
        self.slots
            .entry(slot)
            .or_insert_with(|| classify(None, None))
            .set(Part::Proxy, Some(proxy));
    }

    pub(crate) fn slot(&self, slot: u32) -> Option<SlotRules> {
        self.slots.get(&slot).map(|rules| rules.clone())
    }

    /// Log one line for each slot with a stale part.
    pub(crate) fn log_stale_slots(&self) {
        let mut slots = self
            .slots
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        slots.sort_unstable();
        for slot in slots {
            let Some(rules) = self.slot(slot) else {
                continue;
            };
            let stale = rules
                .iter()
                .filter(|(_, state)| **state == Some(PartState::Stale))
                .map(|(part, _)| part.as_str())
                .collect::<Vec<_>>();
            if !stale.is_empty() {
                tracing::info!(
                    slot_id = slot,
                    stale_parts = stale.join(","),
                    "slot namespace rules differ from this node-manager's"
                );
            }
        }
    }
}

fn classify(stamps: Option<&NamespaceStamps>, current: Option<&PerPart<PartVersion>>) -> SlotRules {
    PerPart::from_fn(|part| {
        let reading = stamps.map(|stamps| *stamps.get(part));
        match (part, reading, current) {
            // Only a listing without the proxy table shows its rules absent.
            (Part::Proxy, Some(StampReading::NoTable), _) => None,
            (_, Some(reading), Some(current)) => {
                Some(PartState::of(reading.version(), *current.get(part)))
            }
            _ => Some(PartState::Unknown),
        }
    })
}

#[cfg(test)]
mod tests;
