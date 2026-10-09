#!/usr/bin/env bash
# Exercise build step environments with real OpenSSH, including ambient config.
# Pass the Linux test executable produced by cargo test --no-run.
set -euo pipefail

if [[ ${1-} != --inside ]]; then
  if [[ $(uname -s) != Linux || $EUID != 0 || $# != 1 || ! -x $1 ]]; then
    echo "Usage: sudo $0 /absolute/path/to/pplx_space_build_mgr-<hash> (Linux)" >&2
    exit 2
  fi
  exec unshare --mount --net --pid --fork --mount-proc "$0" --inside "$1"
fi

binary=$2
mount --make-rprivate /
fixture=$(mktemp -d /var/tmp/space-env-ssh.XXXXXX)
sshd_pid=
cleanup() {
  if [[ -n $sshd_pid ]] && kill -0 "$sshd_pid" 2>/dev/null; then
    kill "$sshd_pid"
    wait "$sshd_pid" || true
  fi
  rm -rf -- "$fixture"
}
trap cleanup EXIT

mount -t tmpfs tmpfs /run
mkdir -p /run/sshd /run/netns /root/.ssh
mount -t tmpfs tmpfs /root/.ssh
ip link set lo up
touch /run/netns/build-env-smoke
mount --bind /proc/self/ns/net /run/netns/build-env-smoke
ssh-keygen -q -t ed25519 -N '' -f "$fixture/client"
ssh-keygen -q -t ed25519 -N '' -f "$fixture/host"
# Shadow accounts/config only inside this disposable mount namespace.
awk 'BEGIN {FS=OFS=":"} $1 == "root" {$7="/bin/dash"} {print}' /etc/passwd > "$fixture/passwd"
mount --bind "$fixture/passwd" /etc/passwd
printf 'Host *\n SendEnv BUILD_SSH_AMBIENT_SECRET\n' > /root/.ssh/config
cp /root/.ssh/config "$fixture/ssh_config"
mount --bind "$fixture/ssh_config" /etc/ssh/ssh_config
export BUILD_SSH_AMBIENT_SECRET=synthetic-ambient-canary

env -i PATH=/usr/sbin:/usr/bin:/bin /usr/sbin/sshd -D -e -f /dev/null -h "$fixture/host" \
  -o "PidFile=$fixture/sshd.pid" -o "AuthorizedKeysFile=$fixture/client.pub" \
  -o StrictModes=no -o PermitRootLogin=yes -o PasswordAuthentication=no \
  -o KbdInteractiveAuthentication=no -o UsePAM=yes \
  -o 'AcceptEnv=*' -o ListenAddress=127.0.0.1 >"$fixture/sshd.log" 2>&1 &
sshd_pid=$!
ssh_args=(-i "$fixture/client" -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)
ready=false
for ((attempt = 0; attempt < 50; attempt++)); do
  if ssh "${ssh_args[@]}" -o ConnectTimeout=1 root@127.0.0.1 true 2>/dev/null; then
    ready=true
    break
  fi
  sleep 0.1
done
if ! $ready; then cat "$fixture/sshd.log" >&2; exit 1; fi

# Controls: the fixture actually accepts ambient SendEnv, and explicit CLI SetEnv.
ssh "${ssh_args[@]}" root@127.0.0.1 'test "$BUILD_SSH_AMBIENT_SECRET" = synthetic-ambient-canary'
ssh -F none "${ssh_args[@]}" -o SetEnv=BUILD_CLI_VALUE=synthetic-explicit \
  root@127.0.0.1 'test "$BUILD_CLI_VALUE" = synthetic-explicit'
echo 'Controls passed: ambient SendEnv and explicit CLI SetEnv reach the guest.'
SPACE_BUILD_SSH_SMOKE_KEY="$fixture/client" SPACE_BUILD_SSH_SMOKE_NETNS=build-env-smoke \
  "$binary" --exact ssh::tests::command_execution_preserves_shell_environment --nocapture
