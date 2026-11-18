# shellcheck shell=bash
# kind-bench.sh — isolated Kind benches side by side on one Linux/KVM host.
#
# A bench is what one CI smoke shard's runner is: a host of its own with a
# Docker daemon of its own, on which the unmodified bring-up and typed smoke run
# with every default name and port (cluster `space-local`, API 127.0.0.1:8080,
# data plane :8090, Tilt :10350). Here each bench is
#
#   - a network namespace `space-bench-<i>`, joined to the host namespace by a
#     veth pair and masqueraded out through it, so every bench can bind the same
#     loopback ports and every kind network can keep Docker's defaults;
#   - a dockerd of its own started inside that namespace, with its own data and
#     exec roots, socket, containerd namespaces and cgroup parent, so Kind node
#     names, images and container state never meet another bench's;
#   - its own HOME and TMPDIR, so kubeconfig, Tilt state, the rendered cluster
#     config and `.kind-data` stay apart;
#   - its own PID namespace for the bench's commands, held open by a holder
#     process. The bring-up finds "its" Tilt and port-forward supervisors by
#     scanning processes (`pgrep`, `lsof`, /proc/<pid>/cwd against the
#     checkout), so with one shared process table every bench would take the
#     others' watchers for its own stale ones and stop them; in its own
#     namespace a bench sees only its processes, as a runner does. Killing the
#     holder ends every process the bench started.
#
# The mount namespace stays shared with the host: the checkout, the staged
# Firecracker vendor tree, /dev/kvm and the shared caches have the same paths in
# every bench, which is what the cluster config's bind mounts need. Commands run
# in a bench through `nsenter --net` rather than `ip netns exec`, which remounts
# /sys for the namespace and leaves /sys/fs/cgroup out of it; dockerd needs the
# cgroup tree. The host's /etc/resolv.conf is used unchanged, so a loopback
# resolver there would be unreachable from a bench: kind_bench_preflight
# refuses that rather than letting every pull fail later.
#
# Sourced, never executed. Needs passwordless sudo; the benches' commands run as
# the calling user.

KIND_BENCH_PREFIX="${KIND_BENCH_PREFIX:-space-bench}"
KIND_BENCH_ROOT="${KIND_BENCH_ROOT:-$HOME/.space-benches}"
# Docker's static release, for hosts whose own daemon is not a local binary
# (a Docker sidecar, a remote DOCKER_HOST). Pinned to the version the bench
# daemons were measured with; any 25+ release with the containerd image store
# works.
KIND_BENCH_DOCKER_VERSION="${KIND_BENCH_DOCKER_VERSION:-29.8.2}"
KIND_BENCH_DOCKER_DIR="${KIND_BENCH_DOCKER_DIR:-/opt/space-bench-docker/bin}"
# The veths take /30s from 192.168.240.0/24 and the daemons /20 pools from
# 192.168.0.0/16 below it, so 15 benches fit. Pools only need to be distinct
# from the host's routes (a bench reaches the host's resolver and egress through
# the host namespace); they are distinct between benches as well so that a
# leaked route or rule names exactly one bench.
export KIND_BENCH_MAX=12

# Written into each bench's directories when they are created. Only a directory
# that carries it is a bench's, so a teardown never deletes a numeric directory
# that merely sits where benches go.
KIND_BENCH_MARKER=.space-bench

kind_bench_log() { printf '[kind-bench] %s\n' "$*" >&2; }
kind_bench_die() { printf '[kind-bench] ERROR: %s\n' "$*" >&2; exit 2; }

kind_bench_ns() { printf '%s-%s\n' "$KIND_BENCH_PREFIX" "$1"; }
kind_bench_dir() { printf '%s/%s\n' "$KIND_BENCH_ROOT" "$1"; }
kind_bench_run_dir() { printf '/run/%s-%s\n' "$KIND_BENCH_PREFIX" "$1"; }
kind_bench_socket() { printf '%s/docker.sock\n' "$(kind_bench_run_dir "$1")"; }
kind_bench_veth() { printf 'sb%sh\n' "$1"; }
kind_bench_host_ip() { printf '192.168.240.%s\n' $(( ($1 - 1) * 4 + 1 )); }
kind_bench_ns_ip() { printf '192.168.240.%s\n' $(( ($1 - 1) * 4 + 2 )); }
kind_bench_link_net() { printf '192.168.240.%s/30\n' $(( ($1 - 1) * 4 )); }
kind_bench_pool() { printf '192.168.%s.0/20\n' $(( ($1 - 1) * 16 )); }
kind_bench_comment() { printf '%s-%s\n' "$KIND_BENCH_PREFIX" "$1"; }

# Hold the host for one run or teardown at a time, on fd 9: bench names
# (namespaces, /run directories, veths, firewall comments, cgroups) are
# host-wide, whatever KIND_BENCH_ROOT a caller chose, and a run's stale-bench
# cleanup would tear down another run's benches under it.
kind_bench_lock() {
  local lock="${KIND_BENCH_LOCK:-}"
  if [[ -z "$lock" ]]; then
    lock="/run/lock/$KIND_BENCH_PREFIX.lock"
    [[ -d /run/lock ]] || lock="/run/$KIND_BENCH_PREFIX.lock"
  fi
  if [[ ! -w "$lock" ]]; then
    sudo -n sh -c 'touch "$1" && chmod 0666 "$1"' _ "$lock" || kind_bench_die "cannot create the host lock $lock"
  fi
  exec 9>>"$lock"
  flock -n 9 || kind_bench_die "another bench run or teardown holds $lock"
}

# Claim bench <i>'s directories: create them and mark them as a bench's.
kind_bench_claim() {
  local dir run
  dir="$(kind_bench_dir "$1")"
  run="$(kind_bench_run_dir "$1")"
  mkdir -p "$dir" && : >"$dir/$KIND_BENCH_MARKER" || return 1
  sudo -n install -d -m 0755 "$run" && sudo -n touch "$run/$KIND_BENCH_MARKER" || return 1
}

# Refuse a host that cannot hold benches, before anything is created.
kind_bench_preflight() {
  sudo -n true 2>/dev/null || kind_bench_die "benches need passwordless sudo (network namespaces, dockerd)"
  [[ -c /dev/kvm && -r /dev/kvm && -w /dev/kvm ]] || kind_bench_die "benches need readable and writable /dev/kvm"
  local tool
  for tool in ip nsenter setpriv iptables; do
    command -v "$tool" >/dev/null 2>&1 || sudo -n sh -c "command -v $tool" >/dev/null 2>&1 \
      || kind_bench_die "benches need $tool on the host"
  done
  if awk '$1 == "nameserver" { print $2 }' /etc/resolv.conf | grep -Eq '^(127\.|::1$)'; then
    kind_bench_die "/etc/resolv.conf names a loopback resolver, which a bench's network namespace cannot reach"
  fi
  local route
  for route in $(ip -4 route show | awk '{print $1}'); do
    case "$route" in
      192.168.*) kind_bench_die "host route $route overlaps the benches' 192.168.0.0/16 addresses" ;;
    esac
  done
}

# The daemon binaries each bench runs: the host's own when it has a local
# dockerd, else Docker's static release unpacked once.
kind_bench_ensure_docker() {
  if [[ -x "$KIND_BENCH_DOCKER_DIR/dockerd" ]]; then
    return 0
  fi
  local tmp
  tmp="$(mktemp -d)" || return 1
  kind_bench_log "installing Docker ${KIND_BENCH_DOCKER_VERSION} daemons into ${KIND_BENCH_DOCKER_DIR}"
  local status=0
  curl -fsSL --retry 3 -o "$tmp/docker.tgz" \
    "https://download.docker.com/linux/static/stable/$(uname -m)/docker-${KIND_BENCH_DOCKER_VERSION}.tgz" \
    || status=1
  (( status )) || tar -C "$tmp" -xzf "$tmp/docker.tgz" || status=1
  (( status )) || sudo -n install -d -m 0755 "$KIND_BENCH_DOCKER_DIR" || status=1
  (( status )) || sudo -n install -m 0755 "$tmp"/docker/* "$KIND_BENCH_DOCKER_DIR/" || status=1
  rm -rf "$tmp"
  return "$status"
}

# Host-global limits that N Kind stacks share and one stack never reaches:
# every node's systemd, kubelet and containerd, and every Tilt watching the
# checkout, take inotify instances and watches from the same per-user pools
# (the nodes all run as root). Only ever raised.
kind_bench_raise_limits() {
  local benches="$1" key want have
  local -A wants=(
    [fs.inotify.max_user_instances]=$(( 2048 * benches > 8192 ? 2048 * benches : 8192 ))
    [fs.inotify.max_user_watches]=$(( 524288 * benches > 1048576 ? 524288 * benches : 1048576 ))
    [fs.aio-max-nr]=1048576
    [net.ipv4.ip_forward]=1
  )
  for key in "${!wants[@]}"; do
    want="${wants[$key]}"
    have="$(sysctl -n "$key" 2>/dev/null || echo 0)"
    if (( have < want )); then
      sudo -n sysctl -qw "$key=$want" || kind_bench_log "WARNING: could not raise $key to $want (have $have)"
    fi
  done
}

# Whether bench <i>'s daemon answers on its socket.
kind_bench_docker_alive() {
  DOCKER_HOST="unix://$(kind_bench_socket "$1")" timeout 10 docker info >/dev/null 2>&1
}

# Network namespace, veth, NAT and forwarding for bench <i>.
kind_bench_network_up() {
  local i="$1" ns veth comment
  ns="$(kind_bench_ns "$i")"
  veth="$(kind_bench_veth "$i")"
  comment="$(kind_bench_comment "$i")"
  sudo -n ip netns add "$ns" || return 1
  sudo -n ip link add "$veth" type veth peer name eth0 netns "$ns" || return 1
  sudo -n ip addr add "$(kind_bench_host_ip "$i")/30" dev "$veth" || return 1
  sudo -n ip link set "$veth" up || return 1
  sudo -n ip -n "$ns" addr add "$(kind_bench_ns_ip "$i")/30" dev eth0 || return 1
  sudo -n ip -n "$ns" link set eth0 up || return 1
  sudo -n ip -n "$ns" link set lo up || return 1
  sudo -n ip -n "$ns" route add default via "$(kind_bench_host_ip "$i")" || return 1
  sudo -n iptables -t nat -A POSTROUTING -s "$(kind_bench_link_net "$i")" ! -o "$veth" \
    -m comment --comment "$comment" -j MASQUERADE || return 1
  # A host Docker daemon's FORWARD policy may be DROP; DOCKER-USER is the chain
  # it leaves to its operator, evaluated before its own rules.
  local chain=FORWARD
  if sudo -n iptables -n -L DOCKER-USER >/dev/null 2>&1; then
    chain=DOCKER-USER
  fi
  sudo -n iptables -I "$chain" -i "$veth" -m comment --comment "$comment" -j ACCEPT || return 1
  sudo -n iptables -I "$chain" -o "$veth" -m comment --comment "$comment" -j ACCEPT || return 1
}

# Bench <i>'s PID namespace (with its own /proc), inside its network
# namespace: a holder process that every bench command joins. Mounts made on
# the host later still reach it (slave propagation).
kind_bench_pidns_up() {
  local i="$1" run holder="" _
  run="$(kind_bench_run_dir "$i")"
  sudo -n install -d -m 0755 "$run" || return 1
  sudo -n nsenter --net="/run/netns/$(kind_bench_ns "$i")" -- \
    setsid unshare --pid --fork --kill-child --mount-proc --propagation slave -- sleep infinity \
    </dev/null >/dev/null 2>&1 &
  # The holder is the `sleep` that is PID 1 of a namespace inside this
  # bench's network namespace.
  local netns pid
  netns="$(sudo -n stat -L -c %i "/run/netns/$(kind_bench_ns "$i")")" || return 1
  for _ in $(seq 1 50); do
    for pid in $(pgrep -x sleep 2>/dev/null); do
      [[ "$(sudo -n stat -L -c %i "/proc/$pid/ns/net" 2>/dev/null)" == "$netns" ]] || continue
      sudo -n awk '$1 == "NSpid:" && $NF == 1 { found = 1 } END { exit !found }' "/proc/$pid/status" 2>/dev/null \
        || continue
      holder="$pid"
    done
    [[ -n "$holder" ]] && break
    sleep 0.2
  done
  [[ -n "$holder" ]] || { kind_bench_log "bench $i: its PID namespace did not start"; return 1; }
  printf '%s\n' "$holder" | sudo -n tee "$run/holder.pid" >/dev/null
}

# The cgroup a bench's processes run in: leaves of the cgroup its daemon
# places its containers under, so the daemon (whose containerd unpacks every
# image the bench loads, and whose page cache holds them), Tilt and the smoke
# runners take their CPU and memory where the bench's Kind nodes do, not from
# the cgroup of the shell that started the run. On a host whose own Docker
# keeps containers outside the caller's limits (a Docker sidecar does), that
# caller may be capped well below the host; a 16-CPU cap throttled four
# benches' image loads (measured with cpu.stat). Prints the cgroup directory.
kind_bench_cgroup() { # <i> <leaf>
  local dir
  dir="/sys/fs/cgroup/$(kind_bench_ns "$1")/$2"
  sudo -n mkdir -p "$dir" && printf '%s\n' "$dir"
}

kind_bench_holder() {
  sudo -n cat "$(kind_bench_run_dir "$1")/holder.pid" 2>/dev/null
}

# Start bench <i>'s dockerd inside its namespace and wait for its socket.
kind_bench_docker_up() {
  local i="$1" ns dir run sock
  ns="$(kind_bench_ns "$i")"
  dir="$(kind_bench_dir "$i")"
  run="$(kind_bench_run_dir "$i")"
  sock="$(kind_bench_socket "$i")"
  # The bench directory is the caller's, so its logs are too; only the data
  # root under it is root's.
  mkdir -p "$dir" || return 1
  sudo -n install -d -m 0755 "$run" "$dir/docker" || return 1
  # The host daemon's image store: the containerd store is what makes the Rust
  # handoff's image ID the producer's manifest digest (kind-ci.yml's
  # enable-containerd-store step).
  # KIND_BENCH_MIRROR, an http://host:port Docker Hub mirror, is plain HTTP
  # on a host-local address, so it is also listed as insecure.
  # cgroupfs: on a systemd host dockerd defaults to the systemd driver,
  # which refuses a --cgroup-parent that is a path.
  jq -n --arg pool "$(kind_bench_pool "$i")" --arg mirror "${KIND_BENCH_MIRROR:-}" '{
      features: {"containerd-snapshotter": true},
      "exec-opts": ["native.cgroupdriver=cgroupfs"],
      "default-address-pools": [{base: $pool, size: 24}],
      "log-opts": {"max-size": "20m", "max-file": "2"}
    } + (if $mirror == "" then {} else {
      "registry-mirrors": [$mirror],
      "insecure-registries": [$mirror | sub("^https?://"; "")]
    } end)' | sudo -n tee "$run/daemon.json" >/dev/null || return 1
  # The bench's own containerd, under its data and run directories: without
  # --containerd, dockerd uses a host containerd it finds at the default socket,
  # which would put the bench's images outside its root and its shims outside
  # its namespaces.
  printf '%s\n' \
    'version = 3' \
    "root = \"$dir/docker/containerd/daemon\"" \
    "state = \"$run/exec/containerd/daemon\"" \
    'disabled_plugins = ["io.containerd.grpc.v1.cri"]' \
    '[grpc]' "  address = \"$run/containerd.sock\"" \
    '[ttrpc]' "  address = \"$run/containerd.sock.ttrpc\"" \
    '[debug]' "  address = \"$run/containerd-debug.sock\"" \
    | sudo -n tee "$run/containerd.toml" >/dev/null || return 1
  # setsid: the daemons outlive the shell that started them, and a Ctrl-C to
  # the run reaches them only through the teardown, in order.
  local cgroup
  cgroup="$(kind_bench_cgroup "$i" daemon)" || return 1
  # shellcheck disable=SC2024 # the log is the caller's file, written as the caller
  sudo -n nsenter --net="/run/netns/$ns" -- env \
    PATH="$KIND_BENCH_DOCKER_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    setsid sh -c 'echo $$ >"$1/cgroup.procs" && echo $$ >"$2" && shift 2 && exec "$@"' _ \
      "$cgroup" "$run/containerd.pid" containerd --config "$run/containerd.toml" \
      </dev/null >"$dir/containerd.log" 2>&1 &
  local _
  for _ in $(seq 1 60); do
    sudo -n test -S "$run/containerd.sock" && break
    sleep 0.5
  done
  sudo -n test -S "$run/containerd.sock" \
    || { tail -n 20 "$dir/containerd.log" >&2; kind_bench_log "bench $i: containerd did not start"; return 1; }
  # shellcheck disable=SC2024 # the log is the caller's file, written as the caller
  sudo -n nsenter --net="/run/netns/$ns" -- env \
    PATH="$KIND_BENCH_DOCKER_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    setsid sh -c 'echo $$ >"$1/cgroup.procs" && shift && exec "$@"' _ "$cgroup" dockerd \
      --config-file "$run/daemon.json" \
      --data-root "$dir/docker" \
      --exec-root "$run/exec" \
      --host "unix://$sock" \
      --pidfile "$run/docker.pid" \
      --containerd "$run/containerd.sock" \
      --containerd-namespace "$ns" \
      --containerd-plugins-namespace "$ns-plugins" \
      --cgroup-parent "/$ns" \
      --userland-proxy=false \
      </dev/null >"$dir/dockerd.log" 2>&1 &
  local _
  for _ in $(seq 1 90); do
    if sudo -n test -S "$sock" && sudo -n chmod 0666 "$sock" 2>/dev/null && kind_bench_docker_alive "$i"; then
      return 0
    fi
    sleep 1
  done
  tail -n 40 "$dir/dockerd.log" >&2 || true
  kind_bench_log "bench $i: dockerd did not answer on $sock within 90 s"
  return 1
}

# The environment every bench command runs with, one KEY=VALUE per line.
# Callers append the run's own settings.
kind_bench_write_env() {
  local i="$1" file="$2" dir
  dir="$(kind_bench_dir "$i")"
  install -d -m 0700 "$dir/home" "$dir/tmp" "$dir/home/.docker" || return 1
  # The host user's registry credentials, so a bench pulls as the host does.
  if [[ -s "${DOCKER_CONFIG:-$HOME/.docker}/config.json" ]]; then
    install -m 0600 "${DOCKER_CONFIG:-$HOME/.docker}/config.json" "$dir/home/.docker/config.json" || return 1
  fi
  {
    printf 'HOME=%s\n' "$dir/home"
    printf 'USER=%s\n' "$(id -un)"
    printf 'LOGNAME=%s\n' "$(id -un)"
    printf 'TMPDIR=%s\n' "$dir/tmp"
    printf 'KUBECONFIG=%s\n' "$dir/home/.kube/config"
    printf 'DOCKER_HOST=unix://%s\n' "$(kind_bench_socket "$i")"
    printf 'DOCKER_CONFIG=%s\n' "$dir/home/.docker"
    printf 'PATH=%s\n' "$PATH"
    printf 'LANG=%s\n' "${LANG:-C.UTF-8}"
    printf 'TERM=%s\n' "${TERM:-dumb}"
    # Shared, read-mostly caches: the host user's own, so a bench neither
    # downloads Python nor resolves packages again.
    printf 'UV_CACHE_DIR=%s\n' "${UV_CACHE_DIR:-$HOME/.cache/uv}"
    printf 'UV_PYTHON_INSTALL_DIR=%s\n' "${UV_PYTHON_INSTALL_DIR:-$HOME/.local/share/uv/python}"
    printf 'SPACE_KIND_BENCH=%s\n' "$i"
  } >"$file" || return 1
}

# Run a command in bench <i> as the calling user, with exactly the variables
# in its env file.
kind_bench_exec() {
  local i="$1" env_file="$2"; shift 2
  local vars=() line
  while IFS= read -r line; do
    [[ -n "$line" && "$line" != \#* ]] && vars+=("$line")
  done <"$env_file"
  local holder
  holder="$(kind_bench_holder "$i")"
  [[ -n "$holder" ]] || { kind_bench_log "bench $i has no PID namespace"; return 2; }
  local cgroup
  cgroup="$(kind_bench_cgroup "$i" commands)" || return 2
  # --wd: entering the mount namespace would otherwise start in its /.
  sudo -n nsenter --target "$holder" --net --pid --mount --wd="$PWD" -- \
    sh -c 'echo $$ >"$1/cgroup.procs" && shift && exec "$@"' _ "$cgroup" \
    setpriv --reuid="$(id -u)" --regid="$(id -g)" --init-groups -- \
    env -i "${vars[@]}" "$@"
}

# A bench's own copy of the checkout <repo> at <dest>, as each CI runner
# checks out its own: the bring-up writes into the tree (Helm vendors chart
# dependencies into charts/ through a tmpcharts-<pid> directory, uv keeps
# .venv), and N benches writing one tree raced; with a PID namespace each,
# their Helm runs even chose the same temporary names. A worktree at <repo>'s
# HEAD with everything that differs from HEAD copied over, staged or not, and
# its untracked files, so a bench tests exactly the files checked out there.
# --no-renames lists a rename as the deletion and the addition it is.
kind_bench_checkout() {
  local repo="$1" dest="$2" lists path status=0
  # NUL-separated lists in files: a shell variable cannot hold a NUL.
  lists="$(mktemp -d)" || return 1
  { git -C "$repo" diff -z --no-renames --name-only --diff-filter=d HEAD >"$lists/changed" \
      && git -C "$repo" ls-files -z --others --exclude-standard >>"$lists/changed" \
      && git -C "$repo" diff -z --no-renames --name-only --diff-filter=D HEAD >"$lists/deleted" \
      && git -C "$repo" worktree add --quiet --detach --force "$dest" HEAD \
      && rsync -a --from0 --files-from="$lists/changed" "$repo/" "$dest/"; } || status=1
  if (( ! status )); then
    while IFS= read -r -d '' path; do
      rm -f -- "$dest/$path" || status=1
    done <"$lists/deleted"
  fi
  rm -rf "$lists"
  return "$status"
}

# Every bench this host has, running or left by an earlier run: namespaces,
# run directories and data directories alike.
kind_bench_existing() {
  {
    sudo -n ip netns list 2>/dev/null | awk '{print $1}' | sed -n "s/^${KIND_BENCH_PREFIX}-\([0-9][0-9]*\)$/\1/p"
    find /run -maxdepth 2 -path "/run/${KIND_BENCH_PREFIX}-*/$KIND_BENCH_MARKER" 2>/dev/null \
      | sed -n "s|^/run/${KIND_BENCH_PREFIX}-\([0-9][0-9]*\)/$KIND_BENCH_MARKER$|\1|p"
    find "$KIND_BENCH_ROOT" -mindepth 2 -maxdepth 2 -name "$KIND_BENCH_MARKER" 2>/dev/null \
      | sed -n "s|^$KIND_BENCH_ROOT/\([0-9][0-9]*\)/$KIND_BENCH_MARKER$|\1|p"
  } | sort -un || true
}

# Remove bench <i> completely: its cluster, every process in its namespace,
# its daemon, mounts, namespace, veth, firewall rules, cgroups and data. Each
# step tolerates what an interrupted run or an earlier step left, so it also
# cleans a half-created bench.
kind_bench_teardown() {
  local _
  local i="$1" ns dir run sock veth comment pid
  ns="$(kind_bench_ns "$i")"
  dir="$(kind_bench_dir "$i")"
  run="$(kind_bench_run_dir "$i")"
  sock="$(kind_bench_socket "$i")"
  veth="$(kind_bench_veth "$i")"
  comment="$(kind_bench_comment "$i")"
  local dockerd_pid=""
  [[ -s "$run/docker.pid" ]] && dockerd_pid="$(sudo -n cat "$run/docker.pid" 2>/dev/null || true)"

  # Tilt, port-forwards and smoke processes first, so nothing recreates what
  # the cluster delete removes. The daemon and its children stay up for it.
  # Every command the bench ran, Tilt and its port-forwards included, dies
  # with its PID namespace's holder.
  local holder
  holder="$(kind_bench_holder "$i")"
  if [[ -n "$holder" ]]; then
    sudo -n kill -KILL "$holder" 2>/dev/null || true
  fi
  # Kind nodes have network namespaces of their own, so what is left in the
  # bench's is the daemon, its containerd and shims, and the run's commands.
  if [[ -e "/run/netns/$ns" ]]; then
    for pid in $(sudo -n ip netns pids "$ns" 2>/dev/null); do
      case "$(ps -o comm= -p "$pid" 2>/dev/null)" in
        dockerd|containerd|containerd-shim*|"") ;;
        *) sudo -n kill -TERM "$pid" 2>/dev/null || true ;;
      esac
    done
  fi
  if kind_bench_docker_alive "$i"; then
    local ids
    ids="$(DOCKER_HOST="unix://$sock" timeout 30 docker ps -aq 2>/dev/null || true)"
    if [[ -n "$ids" ]]; then
      # shellcheck disable=SC2086 # one container ID per word
      DOCKER_HOST="unix://$sock" timeout 180 docker rm -f $ids >/dev/null 2>&1 || true
    fi
  fi
  if [[ -n "$dockerd_pid" ]] && sudo -n kill -0 "$dockerd_pid" 2>/dev/null; then
    sudo -n kill -TERM "$dockerd_pid" 2>/dev/null || true
    local _
    for _ in $(seq 1 30); do
      sudo -n kill -0 "$dockerd_pid" 2>/dev/null || break
      sleep 1
    done
    sudo -n kill -KILL "$dockerd_pid" 2>/dev/null || true
  fi
  local containerd_pid=""
  containerd_pid="$(sudo -n cat "$run/containerd.pid" 2>/dev/null || true)"
  if [[ -n "$containerd_pid" ]] && sudo -n kill -0 "$containerd_pid" 2>/dev/null; then
    sudo -n kill -TERM "$containerd_pid" 2>/dev/null || true
    for _ in $(seq 1 15); do
      sudo -n kill -0 "$containerd_pid" 2>/dev/null || break
      sleep 1
    done
    sudo -n kill -KILL "$containerd_pid" 2>/dev/null || true
  fi
  if [[ -e "/run/netns/$ns" ]]; then
    for pid in $(sudo -n ip netns pids "$ns" 2>/dev/null); do
      sudo -n kill -KILL "$pid" 2>/dev/null || true
    done
  fi
  # A killed daemon leaves its overlay and shm mounts behind; deepest first.
  local mount
  while IFS= read -r mount; do
    if [[ -n "$mount" ]]; then sudo -n umount -l "$mount" 2>/dev/null || true; fi
  done < <(awk -v a="$dir/" -v b="$run/" 'index($2, a) == 1 || index($2, b) == 1 { print $2 }' /proc/self/mounts | sort -r)
  sudo -n iptables -S 2>/dev/null | grep -F -- "--comment $comment " | sed 's/^-A /-D /' \
    | while IFS= read -r rule; do eval "sudo -n iptables $rule" 2>/dev/null || true; done
  sudo -n iptables -t nat -S 2>/dev/null | grep -F -- "--comment $comment " | sed 's/^-A /-D /' \
    | while IFS= read -r rule; do eval "sudo -n iptables -t nat $rule" 2>/dev/null || true; done
  sudo -n ip link del "$veth" 2>/dev/null || true
  sudo -n ip netns del "$ns" 2>/dev/null || true
  # The daemon's cgroup parent, leaves first; a cgroup still holding a process
  # stays, and says so.
  if [[ -d "/sys/fs/cgroup/$ns" ]]; then
    # Whatever is still in the bench's cgroups dies with them.
    if [[ -e "/sys/fs/cgroup/$ns/cgroup.kill" ]]; then
      echo 1 | sudo -n tee "/sys/fs/cgroup/$ns/cgroup.kill" >/dev/null 2>&1 || true
      for _ in $(seq 1 20); do
        [[ -z "$(find "/sys/fs/cgroup/$ns" -name cgroup.procs -exec cat {} + 2>/dev/null)" ]] && break
        sleep 0.5
      done
    fi
    find "/sys/fs/cgroup/$ns" -depth -type d -exec sudo -n rmdir {} \; 2>/dev/null || true
    [[ ! -d "/sys/fs/cgroup/$ns" ]] || kind_bench_log "WARNING: /sys/fs/cgroup/$ns still holds processes"
  fi
  local owned
  for owned in "$run" "$dir"; do
    if [[ -e "$owned/$KIND_BENCH_MARKER" ]] || sudo -n test -e "$owned/$KIND_BENCH_MARKER"; then
      sudo -n rm -rf "$owned"
    elif [[ -e "$owned" ]]; then
      kind_bench_log "WARNING: leaving $owned: it carries no $KIND_BENCH_MARKER marker"
    fi
  done
}
