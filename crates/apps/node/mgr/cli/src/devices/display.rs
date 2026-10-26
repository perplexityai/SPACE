use super::*;
use comfy_table::{Table, presets::UTF8_FULL};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Capacity {
    Available,
    Allocated,
    Cleaning,
    Quarantined,
    Unavailable,
    Unknown,
}
impl Capacity {
    const ALL: [Self; 6] = [
        Self::Available,
        Self::Allocated,
        Self::Cleaning,
        Self::Quarantined,
        Self::Unavailable,
        Self::Unknown,
    ];
    fn label(self) -> &'static str {
        match self {
            Self::Available => "AVAILABLE",
            Self::Allocated => "ALLOCATED",
            Self::Cleaning => "CLEANING",
            Self::Quarantined => "QUARANTINED",
            Self::Unavailable => "UNAVAILABLE",
            Self::Unknown => "UNKNOWN",
        }
    }
}

pub(super) fn owner(allocation: &DeviceAllocationRecord) -> (&'static str, &str) {
    use device_allocation_record::Owner;
    match &allocation.owner {
        Some(Owner::SandboxId(id)) => ("sandbox", id),
        Some(Owner::SanitizerProcedureDigest(digest)) => ("sanitizer", digest),
        None => ("unknown", "-"),
    }
}
pub(super) fn allocation_state(allocation: &DeviceAllocationRecord) -> &'static str {
    match DeviceAllocationState::try_from(allocation.state) {
        Ok(DeviceAllocationState::Reserved) => "reserved",
        Ok(DeviceAllocationState::Attaching) => "attaching",
        Ok(DeviceAllocationState::Attached) => "attached",
        Ok(DeviceAllocationState::Releasing) => "releasing",
        Ok(DeviceAllocationState::Quarantined) => "quarantined",
        _ => "unknown",
    }
}
fn capacity(parent: &DeviceParentRecord) -> Capacity {
    let Some(usage) = &parent.usage else {
        return Capacity::Unknown;
    };
    let Ok(state) = DeviceParentState::try_from(parent.state) else {
        return Capacity::Unknown;
    };
    if state == DeviceParentState::Unspecified
        || parent.host_device_ids.is_empty()
        || usage.allocations.iter().any(|a| {
            allocation_state(a) == "unknown"
                || a.execution_id.is_empty()
                || a.owner.is_none()
                || owner(a).1.is_empty()
                || a.assignment.as_ref().is_none_or(|assignment| {
                    assignment.lease_id.is_empty()
                        || assignment.parent_id != parent.parent_id
                        || assignment.profile != parent.profile
                        || assignment.host_device_ids.is_empty()
                })
        })
        || (usage.allocatable
            && (state != DeviceParentState::Ready || !usage.allocations.is_empty()))
    {
        return Capacity::Unknown;
    }
    if state == DeviceParentState::Quarantined
        || usage
            .allocations
            .iter()
            .any(|a| a.state == DeviceAllocationState::Quarantined as i32)
    {
        return Capacity::Quarantined;
    }
    if state == DeviceParentState::Retired {
        return Capacity::Unavailable;
    }
    if usage
        .allocations
        .iter()
        .any(|a| matches!(a.owner, Some(device_allocation_record::Owner::SandboxId(_))))
    {
        return Capacity::Allocated;
    }
    if state == DeviceParentState::Sanitizing || !usage.allocations.is_empty() {
        return Capacity::Cleaning;
    }
    if usage.allocatable {
        Capacity::Available
    } else {
        Capacity::Unavailable
    }
}

pub(super) fn tables(parents: &[DeviceParentRecord]) -> String {
    let mut summaries = BTreeMap::<&str, BTreeMap<Capacity, usize>>::new();
    let mut rows = Table::new();
    rows.load_style(UTF8_FULL).set_header([
        "PARENT",
        "PROFILE",
        "MEMBERS",
        "STATUS",
        "OWNER",
        "ALLOCATION",
        "EXECUTION",
    ]);
    for parent in parents {
        let status = capacity(parent);
        *summaries
            .entry(&parent.profile)
            .or_default()
            .entry(status)
            .or_default() += 1;
        let allocations = parent
            .usage
            .as_ref()
            .map(|u| u.allocations.as_slice())
            .unwrap_or_default();
        let prefix = [
            parent.parent_id.clone(),
            parent.profile.clone(),
            parent.host_device_ids.join(", "),
            status.label().into(),
        ];
        if allocations.is_empty() {
            rows.add_row(
                prefix
                    .into_iter()
                    .chain(["-".into(), "-".into(), "-".into()]),
            );
        } else {
            for allocation in allocations {
                let (kind, owner) = owner(allocation);
                rows.add_row(prefix.clone().into_iter().chain([
                    format!("{kind}:{owner}"),
                    allocation_state(allocation).into(),
                    allocation.execution_id.clone(),
                ]));
            }
        }
    }
    let mut summary = Table::new();
    summary.load_style(UTF8_FULL).set_header(
        ["PROFILE", "ENROLLED"]
            .into_iter()
            .chain(Capacity::ALL.map(Capacity::label)),
    );
    for (profile, counts) in summaries {
        summary.add_row(
            [
                profile.to_owned(),
                counts.values().sum::<usize>().to_string(),
            ]
            .into_iter()
            .chain(
                Capacity::ALL
                    .map(|state| counts.get(&state).copied().unwrap_or_default().to_string()),
            ),
        );
    }
    format!(
        "Enrolled device units (whole reset domains; not fractional slots):\n{summary}\n\n{rows}"
    )
}

#[cfg(test)]
mod tests;
