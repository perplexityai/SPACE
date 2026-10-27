use crate::host_command::Command;
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::{ExitStatus, Output};
use std::time::Duration;
use std::{fs, thread};

use futures_util::stream::TryStreamExt;
use nix::mount::{MntFlags, MsFlags, mount, umount2};
use nix::sched::{CloneFlags, setns, unshare};
use rtnetlink::packet_route::AddressFamily;
use rtnetlink::packet_route::link::LinkAttribute;
use rtnetlink::packet_route::neighbour::NeighbourState;
use rtnetlink::packet_route::route::{
    RouteAddress, RouteAttribute, RouteMessage, RouteScope, RouteType,
};
use rtnetlink::packet_route::rule::{RuleAction, RuleAttribute, RuleMessage};
use rtnetlink::{Handle, IpVersion, LinkUnspec, LinkVeth, RouteMessageBuilder, new_connection};

use super::{
    DefaultIfaceError, NetId, NetnsEnterError, NetworkOpError, NetworkProbeError, TAP_DEVICE,
    TAP_IP,
};

const NETNS_DIR: &str = "/run/netns";
const NETLINK_TIMEOUT: Duration = Duration::from_secs(5);

thread_local! {
    static IN_NETNS_THREAD: Cell<bool> = const { Cell::new(false) };
}

pub fn add_netns(ns: &str) -> Result<(), NetworkOpError> {
    fs::create_dir_all(NETNS_DIR).map_err(NetworkOpError::from_io_error)?;
    let path = netns_path(ns);
    if Path::new(&path).exists() {
        return Err(NetworkOpError::transient(format!(
            "network namespace {ns} already exists"
        )));
    }
    fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(NetworkOpError::from_io_error)?;

    let path_for_thread = path.clone();
    let result = thread::spawn(move || {
        unshare(CloneFlags::CLONE_NEWNET)
            .map_err(|error| NetworkOpError::transient(error.to_string()))?;
        // This runs in a spawned thread. /proc/self follows the thread-group
        // leader, so use thread-self to bind the namespace we just unshared.
        bind_mount("/proc/thread-self/ns/net", &path_for_thread).map_err(NetworkOpError::transient)
    })
    .join()
    .map_err(|_| {
        NetworkOpError::transient(format!(
            "network namespace creation thread panicked for {ns}"
        ))
    })?;

    if result.is_err() {
        let _ = fs::remove_file(&path);
    }
    result
}

pub fn delete_netns(ns: &str) -> Result<(), String> {
    let path = netns_path(ns);
    if !Path::new(&path).exists() {
        return Ok(());
    }

    if let Err(error) = umount2(path.as_str(), MntFlags::MNT_DETACH)
        && error != nix::errno::Errno::EINVAL
    {
        return Err(format!("failed to unmount namespace {ns}: {error}"));
    }

    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to remove namespace file {path}: {err}")),
    }
}

pub fn setup_host_veth(
    sandbox_ns: &str,
    net: &NetId,
    pod_netns: Option<&str>,
) -> Result<(), NetworkOpError> {
    let sveth = net.sveth();
    let vpeer = net.vpeer();
    let sandbox_ns_path = netns_path(sandbox_ns);
    let (sveth_addr, sveth_prefix) =
        parse_cidr(&net.sveth_ip()).map_err(NetworkOpError::permanent)?;

    let configure = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                handle
                    .link()
                    .add(LinkVeth::new(&sveth, &vpeer).build())
                    .execute()
                    .await
                    .map_err(|e| format!("failed to create veth pair {sveth}/{vpeer}: {e}"))?;

                let vpeer_index = require_link_index(&handle, &vpeer).await?;
                let sandbox_ns_file = fs::File::open(&sandbox_ns_path)
                    .map_err(|e| format!("failed to open sandbox netns {sandbox_ns_path}: {e}"))?;
                handle
                    .link()
                    .set(
                        LinkUnspec::new_with_index(vpeer_index)
                            .setns_by_fd(sandbox_ns_file.as_raw_fd())
                            .build(),
                    )
                    .execute()
                    .await
                    .map_err(|e| format!("failed to move {vpeer} into sandbox netns: {e}"))?;

                let sveth_index = require_link_index(&handle, &sveth).await?;
                handle
                    .address()
                    .add(sveth_index, IpAddr::V4(sveth_addr), sveth_prefix)
                    .replace()
                    .execute()
                    .await
                    .map_err(|e| {
                        format!("failed to add {sveth_addr}/{sveth_prefix} to {sveth}: {e}")
                    })?;
                set_link_up(&handle, sveth_index, &sveth).await?;
                Ok(())
            })
            .await
        })
        .map_err(NetworkOpError::transient)
    };

    if let Some(pod_ns) = pod_netns {
        run_in_netns(pod_ns, configure)
    } else {
        configure()
    }
}

pub fn setup_namespace_links(ns: &str, net: &NetId) -> Result<(), NetworkOpError> {
    let net = NetId::from_slot(net.slot);
    run_in_netns(ns, move || {
        create_persistent_tap(TAP_DEVICE).map_err(NetworkOpError::transient)?;
        configure_namespace_links(&net)
    })
}

pub fn reset_namespace_links(ns: &str, net: &NetId) -> Result<(), String> {
    let net = NetId::from_slot(net.slot);
    run_in_netns(ns, move || {
        configure_namespace_links(&net).map_err(|error| error.to_string())
    })
}

fn configure_namespace_links(net: &NetId) -> Result<(), NetworkOpError> {
    let vpeer = net.vpeer();
    let (vpeer_addr, vpeer_prefix) =
        parse_cidr(&net.vpeer_ip()).map_err(NetworkOpError::permanent)?;
    let (tap_addr, tap_prefix) = parse_cidr(TAP_IP).map_err(NetworkOpError::permanent)?;
    let sveth_addr = parse_ipv4(&net.sveth_ip_bare()).map_err(NetworkOpError::permanent)?;

    block_on_netlink(async move {
        with_handle(|handle| async move {
            let vpeer_index = require_link_index(&handle, &vpeer).await?;
            handle
                .address()
                .add(vpeer_index, IpAddr::V4(vpeer_addr), vpeer_prefix)
                .replace()
                .execute()
                .await
                .map_err(|e| {
                    format!("failed to add {vpeer_addr}/{vpeer_prefix} to {vpeer}: {e}")
                })?;
            set_link_up(&handle, vpeer_index, &vpeer).await?;

            let lo_index = require_link_index(&handle, "lo").await?;
            set_link_up(&handle, lo_index, "lo").await?;

            let tap_index = require_link_index(&handle, TAP_DEVICE).await?;
            handle
                .address()
                .add(tap_index, IpAddr::V4(tap_addr), tap_prefix)
                .replace()
                .execute()
                .await
                .map_err(|e| {
                    format!("failed to add {tap_addr}/{tap_prefix} to {TAP_DEVICE}: {e}")
                })?;
            set_link_up(&handle, tap_index, TAP_DEVICE).await?;

            add_route_to_link(&handle, sveth_addr, 32, vpeer_index)
                .await
                .map_err(|e| format!("failed to add route to {sveth_addr} via {vpeer}: {e}"))?;
            add_default_route(&handle, sveth_addr)
                .await
                .map_err(|e| format!("failed to add default route via {sveth_addr}: {e}"))?;
            Ok(())
        })
        .await
    })
    .map_err(NetworkOpError::transient)
}

pub const NAMESPACE_ICMP_RATEMASK: &str = "6168";

pub fn setup_namespace_sysctls(ns: &str) -> Result<(), NetworkOpError> {
    write_sysctl(Some(ns), "net.ipv4.icmp_ratemask", NAMESPACE_ICMP_RATEMASK)
        .map_err(NetworkOpError::transient)?;
    write_sysctl(Some(ns), "net.ipv4.ip_forward", "1").map_err(NetworkOpError::transient)?;
    write_sysctl(Some(ns), "net.ipv4.conf.tap0.rp_filter", "0")
        .map_err(NetworkOpError::transient)?;
    write_sysctl(Some(ns), "net.ipv4.conf.all.rp_filter", "0")
        .map_err(NetworkOpError::transient)?;
    // A host's src_valid_mark=1 (Tailscale, wg-quick) carries into the namespace and makes the
    // TPROXY'd guest packets martians. Either all or tap0 counts (IN_DEV_ORCONF), so pin both.
    write_sysctl(Some(ns), "net.ipv4.conf.tap0.src_valid_mark", "0")
        .map_err(NetworkOpError::transient)?;
    write_sysctl(Some(ns), "net.ipv4.conf.all.src_valid_mark", "0")
        .map_err(NetworkOpError::transient)
}

pub fn add_host_route(net: &NetId, ns: Option<&str>) -> Result<(), NetworkOpError> {
    let sveth = net.sveth();
    let dest = parse_ipv4(&net.vpeer_ip_bare()).map_err(NetworkOpError::permanent)?;
    let configure = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let sveth_index = require_link_index(&handle, &sveth).await?;
                add_route_to_link(&handle, dest, 32, sveth_index).await
            })
            .await
        })
        .map_err(NetworkOpError::transient)
    };

    if let Some(ns) = ns {
        run_in_netns(ns, configure)
    } else {
        configure()
    }
}

pub fn delete_host_route(net: &NetId, ns: Option<&str>) -> Result<(), String> {
    let sveth = net.sveth();
    let dest = parse_ipv4(&net.vpeer_ip_bare())?;
    let configure = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let Some(sveth_index) = link_index(&handle, &sveth).await? else {
                    return Ok(());
                };
                let route = RouteMessageBuilder::<Ipv4Addr>::new()
                    .destination_prefix(dest, 32)
                    .output_interface(sveth_index)
                    .scope(RouteScope::Link)
                    .build();
                let _ = handle.route().del(route).execute().await;
                Ok(())
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, configure)
    } else {
        configure()
    }
}

pub fn delete_default_route_for_link(name: &str, ns: Option<&str>) -> Result<(), String> {
    let name = name.to_string();
    let configure = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let Some(index) = link_index(&handle, &name).await? else {
                    return Ok(());
                };
                let route = RouteMessageBuilder::<Ipv4Addr>::new()
                    .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
                    .output_interface(index)
                    .build();
                let _ = handle.route().del(route).execute().await;
                Ok(())
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, configure)
    } else {
        configure()
    }
}

pub fn delete_link(name: &str, ns: Option<&str>) -> Result<(), String> {
    let name = name.to_string();
    let configure = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let Some(index) = link_index(&handle, &name).await? else {
                    return Ok(());
                };
                handle
                    .link()
                    .del(index)
                    .execute()
                    .await
                    .map_err(|e| format!("failed to delete link {name}: {e}"))
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, configure)
    } else {
        configure()
    }
}

pub fn add_tproxy_rule_tolerant(ns: &str, mark: u32, table: u32) {
    let ns = ns.to_string();
    let _ = run_in_netns(&ns, move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let _ = handle
                    .rule()
                    .add()
                    .v4()
                    .action(RuleAction::ToTable)
                    .fw_mark(mark)
                    .table_id(table)
                    .execute()
                    .await;
                Ok(())
            })
            .await
        })
    });
}

pub fn replace_tproxy_route(ns: &str, table: u32) -> Result<(), String> {
    run_in_netns(ns, move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let lo_index = require_link_index(&handle, "lo").await?;
                let route = RouteMessageBuilder::<Ipv4Addr>::new()
                    .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
                    .output_interface(lo_index)
                    .table_id(table)
                    .kind(RouteType::Local)
                    .scope(RouteScope::Host)
                    .build();
                handle
                    .route()
                    .add(route)
                    .replace()
                    .execute()
                    .await
                    .map_err(|e| format!("failed to replace TPROXY local route: {e}"))
            })
            .await
        })
    })
}

pub fn clear_tproxy_policy_routing(ns: &str, mark: u32, table: u32) -> Result<(), String> {
    run_in_netns(ns, move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let mut rules = handle.rule().get(IpVersion::V4).execute();
                while let Some(rule) = rules
                    .try_next()
                    .await
                    .map_err(|e| format!("failed to dump IPv4 rules: {e}"))?
                {
                    if rule_fwmark(&rule) == Some(mark) && rule_table(&rule) == Some(table) {
                        let _ = handle.rule().del(rule).execute().await;
                    }
                }

                let mut routes = handle
                    .route()
                    .get(RouteMessageBuilder::<Ipv4Addr>::new().build())
                    .execute();
                while let Some(route) = routes
                    .try_next()
                    .await
                    .map_err(|e| format!("failed to dump IPv4 routes: {e}"))?
                {
                    if route_table(&route) == Some(table) {
                        let _ = handle.route().del(route).execute().await;
                    }
                }
                Ok(())
            })
            .await
        })
    })
}

pub fn flush_neighbours(ns: &str) -> Result<(), String> {
    run_in_netns(ns, move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let mut neighbours = handle
                    .neighbours()
                    .get()
                    .set_address_family(AddressFamily::Inet)
                    .execute();
                while let Some(neighbour) = neighbours
                    .try_next()
                    .await
                    .map_err(|e| format!("failed to dump IPv4 neighbours: {e}"))?
                {
                    if neighbour.header.state == NeighbourState::Permanent {
                        continue;
                    }
                    let _ = handle.neighbours().del(neighbour).execute().await;
                }
                Ok(())
            })
            .await
        })
    })
}

pub(crate) fn command_output_in_ns(ns: &str, program: &str, args: &[&str]) -> io::Result<Output> {
    command_output_with_budget(ns, program, args, &[], crate::host_command::PROBE_BUDGET)
}

pub(crate) fn command_status_in_ns(
    ns: &str,
    program: &str,
    args: &[&str],
) -> io::Result<ExitStatus> {
    command_output_with_stdin_in_ns(ns, program, args, &[]).map(|output| output.status)
}

pub(crate) fn command_output_with_stdin_in_ns(
    ns: &str,
    program: &str,
    args: &[&str],
    stdin: &[u8],
) -> io::Result<Output> {
    command_output_with_budget(
        ns,
        program,
        args,
        stdin,
        crate::host_command::MUTATION_BUDGET,
    )
}

fn command_output_with_budget(
    ns: &str,
    program: &str,
    args: &[&str],
    stdin: &[u8],
    budget: std::time::Duration,
) -> io::Result<Output> {
    let program = program.to_string();
    let args = owned_args(args);
    let stdin = stdin.to_vec();
    run_in_netns(ns, move || {
        crate::host_command::output(Command::new(&program).args(&args), budget, &stdin)
    })
}

pub fn default_iface(ns: Option<&str>) -> Result<String, DefaultIfaceError> {
    let read = || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let route = RouteMessageBuilder::<Ipv4Addr>::new().build();
                let mut routes = handle.route().get(route).execute();
                while let Some(route) = routes
                    .try_next()
                    .await
                    .map_err(|e| format!("failed to dump routes: {e}"))?
                {
                    if route.header.destination_prefix_length != 0 {
                        continue;
                    }
                    if let Some(index) = route.attributes.iter().find_map(route_oif) {
                        return link_name_by_index(&handle, index).await?.ok_or_else(|| {
                            format!("default route output interface {index} not found")
                        });
                    }
                }
                Err("default network interface not found".to_string())
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, move || read().map_err(DefaultIfaceError::Other))
    } else {
        read().map_err(DefaultIfaceError::Other)
    }
}

pub fn ns_exists(name: &str) -> bool {
    Path::new(&netns_path(name)).exists()
}

pub fn link_exists(name: &str, ns: Option<&str>) -> bool {
    try_link_exists(name, ns).unwrap_or(false)
}

pub fn try_link_exists(name: &str, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    let name = name.to_string();
    let query_name = name.clone();
    let read = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                link_index(&handle, &name)
                    .await
                    .map(|index| index.is_some())
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, read).map_err(|detail| NetworkProbeError::Namespace {
            ns: ns.to_string(),
            detail,
        })
    } else {
        read().map_err(|detail| NetworkProbeError::LinkQuery {
            name: query_name,
            detail,
        })
    }
}

pub fn host_route_exists(net: &NetId, ns: Option<&str>) -> bool {
    try_host_route_exists(net, ns).unwrap_or(false)
}

pub fn try_host_route_exists(net: &NetId, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    let sveth = net.sveth();
    let dest = parse_ipv4(&net.vpeer_ip_bare())
        .map_err(|detail| NetworkProbeError::RouteDump { detail })?;
    let read = move || {
        block_on_netlink(async move {
            with_handle(|handle| async move {
                let sveth_index = match link_index(&handle, &sveth).await {
                    Ok(Some(index)) => index,
                    Ok(None) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let route = RouteMessageBuilder::<Ipv4Addr>::new().build();
                let mut routes = handle.route().get(route).execute();
                while let Some(route) = routes
                    .try_next()
                    .await
                    .map_err(|e| format!("failed to dump routes: {e}"))?
                {
                    if route.header.destination_prefix_length == 32
                        && route
                            .attributes
                            .iter()
                            .any(|attr| route_destination(attr) == Some(dest))
                        && route
                            .attributes
                            .iter()
                            .any(|attr| route_oif(attr) == Some(sveth_index))
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .await
        })
    };

    if let Some(ns) = ns {
        run_in_netns(ns, read).map_err(|detail| NetworkProbeError::Namespace {
            ns: ns.to_string(),
            detail,
        })
    } else {
        read().map_err(|detail| NetworkProbeError::RouteDump { detail })
    }
}

pub fn root_default_route_uses_space_link() -> bool {
    try_root_default_route_uses_space_link().unwrap_or(false)
}

pub fn try_root_default_route_uses_space_link() -> Result<bool, NetworkProbeError> {
    block_on_netlink(async move {
        with_handle(|handle| async move {
            let route = RouteMessageBuilder::<Ipv4Addr>::new().build();
            let mut routes = handle.route().get(route).execute();
            while let Some(route) = routes
                .try_next()
                .await
                .map_err(|e| format!("failed to dump routes: {e}"))?
            {
                if route.header.destination_prefix_length != 0 {
                    continue;
                }
                let Some(index) = route.attributes.iter().find_map(route_oif) else {
                    continue;
                };
                let Some(name) = link_name_by_index(&handle, index).await? else {
                    continue;
                };
                if name.starts_with("sveth-") || name.starts_with("vpeer-") {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
    })
    .map_err(|detail| NetworkProbeError::RootDefaultRoute { detail })
}

pub fn read_sysctl(ns: Option<&str>, key: &str) -> Result<String, String> {
    let path = sysctl_path(key);
    let read = move || {
        fs::read_to_string(&path)
            .map_err(|e| format!("failed to read sysctl {}: {e}", path.display()))
    };

    if let Some(ns) = ns {
        run_in_netns(ns, read)
    } else {
        read()
    }
}

pub fn write_sysctl(ns: Option<&str>, key: &str, value: &str) -> Result<(), String> {
    let path = sysctl_path(key);
    let value = value.to_string();
    let write = move || {
        fs::write(&path, &value)
            .map_err(|e| format!("failed to write sysctl {}={value}: {e}", path.display()))
    };

    if let Some(ns) = ns {
        run_in_netns(ns, write)
    } else {
        write()
    }
}

async fn add_route_to_link(
    handle: &Handle,
    destination: Ipv4Addr,
    prefix: u8,
    oif: u32,
) -> Result<(), String> {
    let route = RouteMessageBuilder::<Ipv4Addr>::new()
        .destination_prefix(destination, prefix)
        .output_interface(oif)
        .scope(RouteScope::Link)
        .build();
    handle
        .route()
        .add(route)
        .replace()
        .execute()
        .await
        .map_err(|e| format!("failed to add route {destination}/{prefix}: {e}"))
}

async fn add_default_route(handle: &Handle, gateway: Ipv4Addr) -> Result<(), String> {
    let route = RouteMessageBuilder::<Ipv4Addr>::new()
        .destination_prefix(Ipv4Addr::UNSPECIFIED, 0)
        .gateway(gateway)
        .build();
    handle
        .route()
        .add(route)
        .replace()
        .execute()
        .await
        .map_err(|e| format!("failed to add default route via {gateway}: {e}"))
}

async fn set_link_up(handle: &Handle, index: u32, name: &str) -> Result<(), String> {
    handle
        .link()
        .set(LinkUnspec::new_with_index(index).up().build())
        .execute()
        .await
        .map_err(|e| format!("failed to set link {name} up: {e}"))
}

async fn require_link_index(handle: &Handle, name: &str) -> Result<u32, String> {
    link_index(handle, name)
        .await?
        .ok_or_else(|| format!("link {name} not found"))
}

async fn link_index(handle: &Handle, name: &str) -> Result<Option<u32>, String> {
    let mut links = handle.link().get().match_name(name.to_string()).execute();
    match links.try_next().await {
        Ok(link) => Ok(link.map(|link| link.header.index)),
        Err(rtnetlink::Error::NetlinkError(error))
            if error
                .code
                .is_some_and(|code| code.get().abs() == libc::ENODEV) =>
        {
            Ok(None)
        }
        Err(error) => Err(format!("failed to query link {name}: {error}")),
    }
}

async fn link_name_by_index(handle: &Handle, index: u32) -> Result<Option<String>, String> {
    let mut links = handle.link().get().match_index(index).execute();
    let Some(link) = links
        .try_next()
        .await
        .map_err(|e| format!("failed to query link index {index}: {e}"))?
    else {
        return Ok(None);
    };
    Ok(link.attributes.iter().find_map(|attr| {
        if let LinkAttribute::IfName(name) = attr {
            Some(name.clone())
        } else {
            None
        }
    }))
}

async fn with_handle<F, Fut, T>(f: F) -> Result<T, String>
where
    F: FnOnce(Handle) -> Fut,
    Fut: Future<Output = Result<T, String>>,
{
    let (connection, handle, _) =
        new_connection().map_err(|e| format!("failed to open rtnetlink socket: {e}"))?;
    let connection_task = tokio::spawn(connection);
    let result = f(handle).await;
    connection_task.abort();
    result
}

fn block_on_netlink<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>> + Send + 'static,
    T: Send + 'static,
{
    if IN_NETNS_THREAD.with(Cell::get) {
        return run_netlink_runtime(future);
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        return thread::spawn(move || run_netlink_runtime(future))
            .join()
            .map_err(|_| "netlink runtime thread panicked".to_string())?;
    }

    run_netlink_runtime(future)
}

fn run_netlink_runtime<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|e| format!("failed to build netlink runtime: {e}"))?
        .block_on(async {
            tokio::time::timeout(NETLINK_TIMEOUT, future)
                .await
                .map_err(|_| {
                    format!(
                        "netlink operation timed out after {}s",
                        NETLINK_TIMEOUT.as_secs()
                    )
                })?
        })
}

fn run_in_netns<F, T, E>(ns: &str, f: F) -> Result<T, E>
where
    F: FnOnce() -> Result<T, E> + Send + 'static,
    T: Send + 'static,
    E: From<NetnsEnterError> + Send + 'static,
{
    let ns = ns.to_string();
    let thread_ns = ns.clone();
    thread::spawn(move || {
        let original = fs::File::open("/proc/thread-self/ns/net")
            .map_err(|source| E::from(NetnsEnterError::OpenCurrent { source }))?;
        let target_path = netns_path(&thread_ns);
        let target = fs::File::open(&target_path).map_err(|source| {
            E::from(NetnsEnterError::OpenTarget {
                path: target_path.clone(),
                source,
            })
        })?;

        setns_file(&target).map_err(|detail| E::from(NetnsEnterError::SetNs { detail }))?;
        IN_NETNS_THREAD.with(|cell| cell.set(true));
        let result = f();
        IN_NETNS_THREAD.with(|cell| cell.set(false));
        let restore_result =
            setns_file(&original).map_err(|detail| E::from(NetnsEnterError::SetNs { detail }));
        match (result, restore_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(err), _) => Err(err),
            (Ok(_), Err(err)) => Err(err),
        }
    })
    .join()
    .map_err(|_| E::from(NetnsEnterError::ThreadPanicked { ns }))?
}

fn setns_file(file: &fs::File) -> Result<(), String> {
    setns(file, CloneFlags::CLONE_NEWNET)
        .map_err(|error| format!("failed to switch network namespace: {error}"))
}

fn create_persistent_tap(name: &str) -> Result<(), String> {
    if name.len() >= libc::IFNAMSIZ {
        return Err(format!("tap device name {name} is too long"));
    }

    let tun = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/net/tun")
        .map_err(|e| format!("failed to open /dev/net/tun: {e}"))?;

    let mut req = libc::ifreq {
        ifr_name: [0; libc::IFNAMSIZ],
        ifr_ifru: libc::__c_anonymous_ifr_ifru {
            ifru_flags: (libc::IFF_TAP | libc::IFF_NO_PI) as libc::c_short,
        },
    };
    for (idx, byte) in name.as_bytes().iter().enumerate() {
        if let Some(slot) = req.ifr_name.get_mut(idx) {
            *slot = *byte as libc::c_char;
        }
    }

    // SAFETY: `tun` is an open /dev/net/tun fd and `req` points to a valid
    // full kernel ifreq buffer, including its union, for TUNSETIFF output.
    if unsafe { libc::ioctl(tun.as_raw_fd(), libc::TUNSETIFF, &mut req) } != 0 {
        return Err(format!(
            "failed to create tap device {name}: {}",
            io::Error::last_os_error()
        ));
    }

    let persist: libc::c_int = 1;
    // SAFETY: `tun` is the fd returned by TUNSETIFF above; TUNSETPERSIST
    // takes an integer flag by value.
    if unsafe { libc::ioctl(tun.as_raw_fd(), libc::TUNSETPERSIST, persist) } != 0 {
        return Err(format!(
            "failed to persist tap device {name}: {}",
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

fn bind_mount(source: &str, target: &str) -> Result<(), String> {
    mount(
        Some(source),
        target,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )
    .map_err(|error| format!("failed to bind mount namespace: {error}"))
}

fn route_oif(attr: &RouteAttribute) -> Option<u32> {
    if let RouteAttribute::Oif(index) = attr {
        Some(*index)
    } else {
        None
    }
}

fn route_destination(attr: &RouteAttribute) -> Option<Ipv4Addr> {
    if let RouteAttribute::Destination(RouteAddress::Inet(addr)) = attr {
        Some(*addr)
    } else {
        None
    }
}

fn route_table(route: &RouteMessage) -> Option<u32> {
    route
        .attributes
        .iter()
        .find_map(|attr| {
            if let RouteAttribute::Table(table) = attr {
                Some(*table)
            } else {
                None
            }
        })
        .or_else(|| Some(route.header.table.into()))
}

fn rule_fwmark(rule: &RuleMessage) -> Option<u32> {
    rule.attributes.iter().find_map(|attr| {
        if let RuleAttribute::FwMark(mark) = attr {
            Some(*mark)
        } else {
            None
        }
    })
}

fn rule_table(rule: &RuleMessage) -> Option<u32> {
    rule.attributes
        .iter()
        .find_map(|attr| {
            if let RuleAttribute::Table(table) = attr {
                Some(*table)
            } else {
                None
            }
        })
        .or_else(|| Some(rule.header.table.into()))
}

fn parse_cidr(value: &str) -> Result<(Ipv4Addr, u8), String> {
    let (addr, prefix) = value
        .split_once('/')
        .ok_or_else(|| format!("invalid IPv4 CIDR {value}"))?;
    let addr = parse_ipv4(addr)?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|e| format!("invalid IPv4 prefix in {value}: {e}"))?;
    Ok((addr, prefix))
}

fn parse_ipv4(value: &str) -> Result<Ipv4Addr, String> {
    value
        .parse::<Ipv4Addr>()
        .map_err(|e| format!("invalid IPv4 address {value}: {e}"))
}

fn sysctl_path(key: &str) -> std::path::PathBuf {
    Path::new("/proc/sys").join(key.replace('.', "/"))
}

fn netns_path(ns: &str) -> String {
    format!("{NETNS_DIR}/{ns}")
}

fn owned_args(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_string()).collect()
}
