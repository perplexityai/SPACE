#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(unix))]
mod other;
#[cfg(all(unix, not(target_os = "linux")))]
mod unix;

#[cfg(target_os = "linux")]
pub(super) use linux::{GuestAccount, ResolveGuestAccountError};
#[cfg(not(unix))]
pub(super) use other::{GuestAccount, ResolveGuestAccountError};
#[cfg(all(unix, not(target_os = "linux")))]
pub(super) use unix::{GuestAccount, ResolveGuestAccountError};

#[cfg(any(target_os = "linux", test))]
fn supplementary_groups_match(
    mut current: Vec<nix::unistd::Gid>,
    requested: &[nix::unistd::Gid],
    primary: nix::unistd::Gid,
) -> bool {
    // getgrouplist includes the primary group; getgroups need not include it.
    current.push(primary);
    current.sort_unstable_by_key(|gid| gid.as_raw());
    current.dedup();
    let mut requested = requested.to_vec();
    requested.push(primary);
    requested.sort_unstable_by_key(|gid| gid.as_raw());
    requested.dedup();
    current == requested
}

#[cfg(test)]
mod tests;
