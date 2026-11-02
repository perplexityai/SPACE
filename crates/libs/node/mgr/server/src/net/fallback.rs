use std::io::{self, Write};
use std::process::{Command, ExitStatus, Output, Stdio};

use super::{DefaultIfaceError, NetId, NetworkOpError, NetworkProbeError, TAP_DEVICE, TAP_IP};

pub fn add_netns(ns: &str) -> Result<(), NetworkOpError> {
    run("ip", &["netns", "add", ns]).map_err(NetworkOpError::transient)
}

pub fn delete_netns(ns: &str) -> Result<(), String> {
    run("ip", &["netns", "delete", ns])
}

pub fn setup_host_veth(
    ns: &str,
    net: &NetId,
    pod_netns: Option<&str>,
) -> Result<(), NetworkOpError> {
    run_ip_batch(
        pod_netns,
        &[
            format!(
                "link add {} type veth peer name {}",
                net.sveth(),
                net.vpeer()
            ),
            format!("link set {} netns {ns}", net.vpeer()),
            format!("addr add {} dev {}", net.sveth_ip(), net.sveth()),
            format!("link set {} up", net.sveth()),
        ],
    )
    .map_err(NetworkOpError::transient)
}

pub fn setup_namespace_links(ns: &str, net: &NetId) -> Result<(), NetworkOpError> {
    run_ip_batch(
        Some(ns),
        &[
            format!("addr add {} dev {}", net.vpeer_ip(), net.vpeer()),
            format!("link set {} up", net.vpeer()),
            "link set lo up".to_string(),
            format!("tuntap add dev {TAP_DEVICE} mode tap"),
            format!("addr add {TAP_IP} dev {TAP_DEVICE}"),
            format!("link set {TAP_DEVICE} up"),
            format!("route add {} dev {}", net.sveth_ip_bare(), net.vpeer()),
            format!("route add default via {}", net.sveth_ip_bare()),
        ],
    )
    .map_err(NetworkOpError::transient)
}

pub fn reset_namespace_links(ns: &str, net: &NetId) -> Result<(), String> {
    run_ip_batch(
        Some(ns),
        &[
            format!("addr replace {} dev {}", net.vpeer_ip(), net.vpeer()),
            format!("link set {} up", net.vpeer()),
            "link set lo up".to_string(),
            format!("addr replace {TAP_IP} dev {TAP_DEVICE}"),
            format!("link set {TAP_DEVICE} up"),
            format!("route replace {} dev {}", net.sveth_ip_bare(), net.vpeer()),
            format!("route replace default via {}", net.sveth_ip_bare()),
        ],
    )
}

pub const NAMESPACE_ICMP_RATEMASK: &str = "6168";

pub fn setup_namespace_sysctls(ns: &str) -> Result<(), NetworkOpError> {
    run_in_ns(
        ns,
        "sysctl",
        &[
            "-q",
            "-w",
            &format!("net.ipv4.icmp_ratemask={NAMESPACE_ICMP_RATEMASK}"),
            "net.ipv4.ip_forward=1",
            "net.ipv4.conf.tap0.rp_filter=0",
            "net.ipv4.conf.all.rp_filter=0",
            "net.ipv4.conf.tap0.src_valid_mark=0",
            "net.ipv4.conf.all.src_valid_mark=0",
        ],
    )
    .map_err(NetworkOpError::transient)
}

pub fn add_host_route(net: &NetId, ns: Option<&str>) -> Result<(), NetworkOpError> {
    if let Some(ns) = ns {
        run_in_ns(
            ns,
            "ip",
            &["route", "add", &net.vpeer_ip_bare(), "dev", &net.sveth()],
        )
    } else {
        run(
            "ip",
            &["route", "add", &net.vpeer_ip_bare(), "dev", &net.sveth()],
        )
    }
    .map_err(NetworkOpError::transient)
}

pub fn delete_host_route(net: &NetId, ns: Option<&str>) -> Result<(), String> {
    if let Some(ns) = ns {
        run_in_ns(ns, "ip", &["route", "delete", &net.vpeer_ip_bare()])
    } else {
        run("ip", &["route", "delete", &net.vpeer_ip_bare()])
    }
}

pub fn delete_default_route_for_link(name: &str, ns: Option<&str>) -> Result<(), String> {
    if let Some(ns) = ns {
        run_in_ns(ns, "ip", &["route", "delete", "default", "dev", name])
    } else {
        run("ip", &["route", "delete", "default", "dev", name])
    }
}

pub fn delete_link(name: &str, ns: Option<&str>) -> Result<(), String> {
    if let Some(ns) = ns {
        run_in_ns(ns, "ip", &["link", "delete", name])
    } else {
        run("ip", &["link", "delete", name])
    }
}

pub fn add_tproxy_rule_tolerant(ns: &str, mark: u32, table: u32) {
    let mark = mark.to_string();
    let table = table.to_string();
    run_in_ns_tolerant(ns, "ip", &["rule", "add", "fwmark", &mark, "table", &table]);
}

pub fn replace_tproxy_route(ns: &str, table: u32) -> Result<(), String> {
    let table = table.to_string();
    run_in_ns(
        ns,
        "ip",
        &[
            "route", "replace", "local", "default", "dev", "lo", "table", &table,
        ],
    )
}

pub fn clear_tproxy_policy_routing(ns: &str, mark: u32, table: u32) -> Result<(), String> {
    let mark = mark.to_string();
    let table = table.to_string();
    for _ in 0..32 {
        let Ok(status) = Command::new("ip")
            .args([
                "netns", "exec", ns, "ip", "rule", "del", "fwmark", &mark, "table", &table,
            ])
            .stderr(std::process::Stdio::null())
            .status()
        else {
            break;
        };
        if !status.success() {
            break;
        }
    }
    run_in_ns(ns, "ip", &["route", "flush", "table", &table])
}

pub fn flush_neighbours(ns: &str) -> Result<(), String> {
    run_in_ns(ns, "ip", &["neigh", "flush", "all"])
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
    let mut command = crate::host_command::Command::new("ip");
    command.args(["netns", "exec", ns, program]).args(args);
    crate::host_command::output(&mut command, budget, stdin)
}

pub fn default_iface(ns: Option<&str>) -> Result<String, DefaultIfaceError> {
    let output = if let Some(ns) = ns {
        // The `ip netns exec` shell-out loses the open(2) errno, so probe the
        // namespace explicitly to classify a missing-namespace failure the way
        // the Linux implementation does.
        if !ns_exists(ns) {
            return Err(DefaultIfaceError::NetnsMissing {
                path: format!("/run/netns/{ns}"),
                source: io::Error::from(io::ErrorKind::NotFound),
            });
        }
        Command::new("ip")
            .args([
                "netns",
                "exec",
                ns,
                "sh",
                "-c",
                "ip route | awk '/default/ {print $5; exit}'",
            ])
            .output()
    } else {
        Command::new("sh")
            .args(["-c", "ip route | awk '/default/ {print $5; exit}'"])
            .output()
    };
    output
        .map_err(|e| DefaultIfaceError::Other(format!("cannot run ip route: {e}")))
        .and_then(|o| {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if s.is_empty() {
                Err(DefaultIfaceError::Other(
                    "default network interface not found".to_string(),
                ))
            } else {
                Ok(s)
            }
        })
}

pub fn ns_exists(name: &str) -> bool {
    Command::new("ip")
        .args(["netns", "list"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .any(|l| l.split_whitespace().next() == Some(name))
        })
        .unwrap_or(false)
}

pub fn link_exists(name: &str, ns: Option<&str>) -> bool {
    try_link_exists(name, ns).unwrap_or(false)
}

pub fn try_link_exists(name: &str, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    let output = if let Some(ns) = ns {
        Command::new("ip")
            .args(["netns", "exec", ns, "ip", "link", "show", name])
            .output()
    } else {
        Command::new("ip").args(["link", "show", name]).output()
    };
    output
        .map_err(|error| NetworkProbeError::LinkQuery {
            name: name.to_string(),
            detail: error.to_string(),
        })
        .and_then(|output| {
            if output.status.success() {
                Ok(true)
            } else if fallback_link_absent(&output) {
                Ok(false)
            } else {
                Err(NetworkProbeError::LinkQuery {
                    name: name.to_string(),
                    detail: String::from_utf8_lossy(&output.stderr).into_owned(),
                })
            }
        })
}

fn fallback_link_absent(output: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    stderr.contains("does not exist")
        || stderr.contains("Device does not exist")
        || stderr.contains("Cannot find device")
        || stderr.contains("No such device")
}

pub fn host_route_exists(net: &NetId, ns: Option<&str>) -> bool {
    try_host_route_exists(net, ns).unwrap_or(false)
}

pub fn try_host_route_exists(net: &NetId, ns: Option<&str>) -> Result<bool, NetworkProbeError> {
    let needle = format!("{} dev {}", net.vpeer_ip_bare(), net.sveth());
    let output = if let Some(ns) = ns {
        Command::new("ip")
            .args(["netns", "exec", ns, "ip", "route", "list"])
            .output()
    } else {
        Command::new("ip").args(["route", "list"]).output()
    };
    output
        .map_err(|error| NetworkProbeError::RouteDump {
            detail: error.to_string(),
        })
        .and_then(|output| {
            if output.status.success() {
                Ok(String::from_utf8_lossy(&output.stdout).contains(&needle))
            } else {
                Err(NetworkProbeError::RouteDump {
                    detail: String::from_utf8_lossy(&output.stderr).into_owned(),
                })
            }
        })
}

pub fn root_default_route_uses_space_link() -> bool {
    try_root_default_route_uses_space_link().unwrap_or(false)
}

pub fn try_root_default_route_uses_space_link() -> Result<bool, NetworkProbeError> {
    let output = Command::new("ip")
        .args(["route", "show", "default"])
        .output()
        .map_err(|error| NetworkProbeError::RootDefaultRoute {
            detail: error.to_string(),
        })?;
    interpret_default_route_output(output.status.success(), &output.stdout, &output.stderr)
}

fn interpret_default_route_output(
    success: bool,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<bool, NetworkProbeError> {
    if !success {
        return Err(NetworkProbeError::RootDefaultRoute {
            detail: String::from_utf8_lossy(stderr).into_owned(),
        });
    }
    Ok(default_route_output_uses_space_link(
        &String::from_utf8_lossy(stdout),
    ))
}

fn default_route_output_uses_space_link(output: &str) -> bool {
    output.lines().any(|line| {
        let mut fields = line.split_whitespace();
        while let Some(field) = fields.next() {
            if field == "dev" {
                return fields.next().is_some_and(|iface| {
                    iface.starts_with("sveth-") || iface.starts_with("vpeer-")
                });
            }
        }
        false
    })
}

pub fn read_sysctl(ns: Option<&str>, key: &str) -> Result<String, String> {
    let output = if let Some(ns) = ns {
        Command::new("ip")
            .args(["netns", "exec", ns, "sysctl", "-n", key])
            .output()
    } else {
        Command::new("sysctl").args(["-n", key]).output()
    };
    output
        .map_err(|e| format!("failed to run sysctl {key}: {e}"))
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
}

pub fn write_sysctl(ns: Option<&str>, key: &str, value: &str) -> Result<(), String> {
    let assignment = format!("{key}={value}");
    if let Some(ns) = ns {
        run_in_ns(ns, "sysctl", &["-q", "-w", &assignment])
    } else {
        run("sysctl", &["-q", "-w", &assignment])
    }
}
fn run_in_ns(ns: &str, program: &str, args: &[&str]) -> Result<(), String> {
    let mut full_args: Vec<&str> = vec!["netns", "exec", ns, program];
    full_args.extend_from_slice(args);
    run("ip", &full_args)
}

fn run_ip_batch(namespace: Option<&str>, commands: &[String]) -> Result<(), String> {
    if commands.is_empty() {
        return Ok(());
    }

    let mut args = Vec::new();
    if let Some(ns) = namespace {
        args.push("-n");
        args.push(ns);
    }
    args.push("-batch");
    args.push("-");

    let mut child = Command::new("ip")
        .args(&args)
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to run ip {}: {e}", args.join(" ")))?;

    {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| "failed to open ip batch stdin".to_string())?;
        for command in commands {
            writeln!(stdin, "{command}")
                .map_err(|e| format!("failed to write ip batch command {command}: {e}"))?;
        }
    }

    let status = child
        .wait()
        .map_err(|e| format!("failed waiting for ip {}: {e}", args.join(" ")))?;
    if !status.success() {
        return Err(format!(
            "ip {} failed for batch: {}",
            args.join(" "),
            commands.join("; ")
        ));
    }
    Ok(())
}

/// Like run_in_ns but tolerates failure (e.g., "RTNETLINK answers: File exists").
fn run_in_ns_tolerant(ns: &str, program: &str, args: &[&str]) {
    let mut full_args: Vec<&str> = vec!["netns", "exec", ns, program];
    full_args.extend_from_slice(args);
    if let Err(error) = Command::new("ip").args(&full_args).status() {
        tracing::debug!(ns, program, args = ?args, %error, "tolerant netns command failed to start");
    }
}

fn run(program: &str, args: &[&str]) -> Result<(), String> {
    let status = Command::new(program)
        .args(args)
        .status()
        .map_err(|e| format!("failed to run {program}: {e}"))?;
    if !status.success() {
        return Err(format!("{program} {} failed", args.join(" ")));
    }
    Ok(())
}

pub(crate) fn guest_tcp_stream(
    _ns: &str,
    _port: std::num::NonZeroU16,
) -> std::io::Result<std::net::TcpStream> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "guest channels require Linux namespaces",
    ))
}

pub(crate) fn guest_dhcp_socket(_ns: &str) -> std::io::Result<std::net::UdpSocket> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "guest DHCP requires Linux namespaces",
    ))
}
