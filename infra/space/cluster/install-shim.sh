#!/usr/bin/env bash
# install-shim.sh — install a built containerd-shim-space-v2 and its wrapper
# on the node it runs on (README.md, Nodes). Run as root by the node's own
# provisioning; it reaches no cluster and never restarts containerd.
#
#   install-shim.sh --binary PATH [--version V] [--root DIR]
#   install-shim.sh --binary PATH [--version V] --print-wrapper
#
#   --binary         the shim built from shim/ (CGO_ENABLED=0 go build)
#   --version        the version the shim reports (SPACE_VERSION and
#                    DD_VERSION; default: first 12 hex of the binary's sha256)
#   --root           install under DIR instead of / (a disposable test root)
#   --print-wrapper  print the wrapper and exit
#
# The wrapper starts with metrics and traces off, then reads the node's
# telemetry settings from /space/config/shim-telemetry.env, which
# space-node-manager-local's node-env init container writes on the
# global.observability path. Without that file the shim exports nothing.
set -euo pipefail

binary=
version=
root=
print=false
while (($#)); do
  case "$1" in
    --binary) (($# >= 2)) || exit 2; binary=$2; shift 2 ;;
    --version) (($# >= 2)) || exit 2; version=$2; shift 2 ;;
    --root) (($# >= 2)) || exit 2; root=${2%/}; shift 2 ;;
    --print-wrapper) print=true; shift ;;
    -h|--help) sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "install-shim.sh: unsupported argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n $binary && -f $binary ]] || { echo "install-shim.sh: --binary must name the built shim" >&2; exit 2; }
[[ -n $version ]] || version=$(sha256sum "$binary" | cut -c1-12)
if [[ ! $version =~ ^[A-Za-z0-9._+-]+$ ]]; then
  echo "install-shim.sh: --version may contain only letters, digits and ._+-" >&2
  exit 2
fi

real=/usr/local/lib/space/containerd-shim-space-v2
wrapper=/usr/local/bin/containerd-shim-space-v2
settings=/space/config/shim-telemetry.env

wrapper_text() {
  cat <<EOF
#!/usr/bin/env bash
set -euo pipefail
# Off unless the node's settings select a mode (install-shim.sh).
export SPACE_METRICS_SINK=off
export SPACE_TRACE_EXPORTER=off
if [[ -r $settings ]]; then
  . $settings
fi
# The settings are node-manager's; the shim reports its own identity, which
# its resolver prefers over DD_* (shim/identity.go).
unset OTEL_SERVICE_NAME
export SPACE_SERVICE_NAME=containerd-shim-space-v2
export DD_SERVICE=containerd-shim-space-v2
export SPACE_VERSION=$version
export DD_VERSION=$version
exec $real "\$@"
EOF
}

if [[ $print == true ]]; then
  wrapper_text
  exit 0
fi

install -d -m 0755 "$root${real%/*}" "$root${wrapper%/*}"
install -m 0755 "$binary" "$root$real.tmp"
mv -f "$root$real.tmp" "$root$real"
wrapper_text >"$root$wrapper.tmp"
chmod 0755 "$root$wrapper.tmp"
mv -f "$root$wrapper.tmp" "$root$wrapper"
echo "install-shim.sh: installed $root$real ($version) and $root$wrapper"

config=$root/etc/containerd/config.toml
if ! grep -qs 'io.containerd.space.v2' "$config"; then
  # containerd 2.x (config version 3) moved the CRI runtimes to
  # io.containerd.cri.v1.runtime; 1.x configs (version 2) keep
  # io.containerd.grpc.v1.cri. The EKS worker bootstrap writes the 2.x form
  # (infra/space/aws/eks/modules/space/templates/worker-bootstrap.sh.tftpl).
  plugin=io.containerd.grpc.v1.cri
  if grep -qsE '^[[:space:]]*version[[:space:]]*=[[:space:]]*3' "$config"; then
    plugin=io.containerd.cri.v1.runtime
  fi
  cat >&2 <<EOF
install-shim.sh: containerd has no \`space\` runtime yet. Add, then restart containerd:
  [plugins."$plugin".containerd.runtimes.space]
    runtime_type = "io.containerd.space.v2"
    pod_annotations = ["space.pplx.net/*"]
EOF
fi
