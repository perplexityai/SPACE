#!/usr/bin/env bash
# Install auditd and configure audit rules for SPACE node security monitoring.
# Runs as root during Packer build.
#
# Logs: /var/log/audit/audit.log (managed by auditd, rotated automatically)
# View: ausearch -k <key>      (e.g. ausearch -k space-binary-tamper)
# Report: aureport --summary
set -euxo pipefail

export DEBIAN_FRONTEND=noninteractive
AL2023_RELEASEVER="${AL2023_RELEASEVER:-latest}"

if command -v apt-get >/dev/null 2>&1; then
  apt-get install -y auditd audispd-plugins 2>/dev/null || apt-get install -y auditd
elif command -v dnf >/dev/null 2>&1; then
  dnf --releasever="$AL2023_RELEASEVER" install -y audit
else
  echo "No supported package manager found" >&2
  exit 1
fi

# ── Audit daemon config ─────────────────────────────────────────────────────
# Keep defaults but ensure enough buffer for busy sandbox nodes.
sed -i 's/^num_logs =.*/num_logs = 8/' /etc/audit/auditd.conf
sed -i 's/^max_log_file =.*/max_log_file = 32/' /etc/audit/auditd.conf
sed -i 's/^max_log_file_action =.*/max_log_file_action = ROTATE/' /etc/audit/auditd.conf

# ── SPACE audit rules ───────────────────────────────────────────────────────
tee /etc/audit/rules.d/90-space.rules > /dev/null <<'EOF'
## ==========================================================================
## SPACE platform audit rules
## Keys use "space-" prefix for easy filtering: ausearch -k space-*
## ==========================================================================

## ── 1. Privilege escalation ─────────────────────────────────────────────────
## Log all sudo executions
-a always,exit -F path=/usr/bin/sudo -F perm=x -k space-sudo
## Log setuid/setgid syscalls (privilege changes in sandbox launch path)
-a always,exit -F arch=b64 -S setuid -S setgid -S setreuid -S setregid -k space-privesc
## Log capability changes
-a always,exit -F arch=b64 -S capset -k space-capset

## ── 2. Binary integrity ────────────────────────────────────────────────────
## Detect writes to SPACE binaries (firecracker, jailer, services)
-w /space/bin/ -p wa -k space-binary-tamper
## Detect writes to Firecracker/jailer vendor binaries and the node's
## artifact store, which only node-manager writes
-w /space/vendor/ -p wa -k space-vendor-tamper

## ── 3. Config integrity ────────────────────────────────────────────────────
## SPACE platform config
-w /space/etc/ -p wa -k space-config-tamper
## Sudoers modifications
-w /etc/sudoers -p wa -k space-sudoers
-w /etc/sudoers.d/ -p wa -k space-sudoers
## Sysctl modifications (affects hugepages, ip_forward)
-w /etc/sysctl.d/ -p wa -k space-sysctl

## ── 4. User/group account changes ──────────────────────────────────────────
-w /etc/passwd -p wa -k space-identity
-w /etc/group -p wa -k space-identity
-w /etc/shadow -p wa -k space-identity
-w /etc/subuid -p wa -k space-identity
-w /etc/subgid -p wa -k space-identity

## ── 5. SSH key access ──────────────────────────────────────────────────────
## Sandbox private key — only root/builder should read this
-w /root/.ssh/sandbox_ed25519 -p r -k space-ssh-key
-w /space/etc/ssh/ -p wa -k space-ssh-config

## ── 6. Namespace & isolation syscalls ──────────────────────────────────────
## Log setns (entering namespaces) and unshare (creating namespaces)
## Exclude node-manager (UID 0, expected) — alert on unexpected callers
-a always,exit -F arch=b64 -S setns -S unshare -F auid>=1000 -F auid!=4294967295 -k space-namespace
## Log clone with CLONE_NEWNET from non-root users. This filters on clone(2),
## whose first argument is the flags word.
-a always,exit -F arch=b64 -S clone -F a0&0x40000000 -F auid>=1000 -F auid!=4294967295 -k space-namespace
## clone3 can create a namespace directly, but its flags sit in a struct its
## first argument points to, which audit filters can't read. So log every
## clone3 from a logged-in user (services carry no auid and are excluded),
## under its own key, and filter for namespace flags when reading the log.
-a always,exit -F arch=b64 -S clone3 -F auid>=1000 -F auid!=4294967295 -k space-namespace-clone3

## ── 7. KVM device access ───────────────────────────────────────────────────
## Track who opens /dev/kvm (VM creation)
-w /dev/kvm -p r -k space-kvm-access

## ── 8. Service socket directories ──────────────────────────────────────────
## Host-local control sockets live in service-owned directories. Track metadata
## changes because unexpected socket replacement indicates compromise.
-w /space/run/node-manager -p wa -k space-nodemgr-socket
-w /space/run/sandbox-manager -p wa -k space-sbxmgr-socket
-w /space/run/sandbox-host-control -p wa -k space-sbxmgr-host-control
-w /space/run/credential-manager -p wa -k space-credmgr-socket
-w /space/run/template-manager -p wa -k space-tmplmgr-socket
## SQLite shares this directory; audit only socket objects, including a
## socket created after rules load, rather than ordinary DB/WAL writes.
-a always,exit -F dir=/space/run/runtime-manager -F filetype=socket -F perm=wa -k space-rtmgr-socket

## ── 9. Sandbox escape indicators ───────────────────────────────────────────
## Sandbox UIDs (10000+) accessing sensitive host paths outside their sandbox dir.
## These should never happen under normal operation.
-a always,exit -F arch=b64 -S openat -F dir=/etc -F auid>=10000 -F auid!=4294967295 -k space-sbx-escape
-a always,exit -F arch=b64 -S openat -F dir=/root -F auid>=10000 -F auid!=4294967295 -k space-sbx-escape

## ── 10. Firecracker runtime ────────────────────────────────────────────────
## Track jailer invocations
-w /space/bin/jailer -p x -k space-jailer-exec
## Track direct firecracker invocations (should go through jailer in prod)
-w /space/bin/firecracker -p x -k space-fc-exec

## ── 11. Module loading ─────────────────────────────────────────────────────
## Kernel module loads could weaken isolation
-a always,exit -F arch=b64 -S init_module -S finit_module -S delete_module -k space-kmod
EOF

# ── Enable and start ────────────────────────────────────────────────────────
# AL2023's audit package ships rules.d/audit.rules ending in `-a task,never`,
# which turns syscall auditing off for every task started after the rules load,
# so the SPACE syscall rules above would load and then never fire. Keep the
# rest of that file (-D, buffer and failure settings) and drop that rule.
if [[ -f /etc/audit/rules.d/audit.rules ]]; then
  sed -i '/^-a[[:space:]]*task,never/d' /etc/audit/rules.d/audit.rules
fi
systemctl enable auditd
# Load the merged rules now, so a rule the kernel rejects fails the bake
# instead of shipping silently.
augenrules --load
if grep -q '^-a[[:space:]]*task,never' /etc/audit/audit.rules; then
  echo "ERROR: /etc/audit/audit.rules still disables syscall auditing (-a task,never)" >&2
  exit 1
fi

echo "=== Auditd configured ==="
echo "  Rules:  /etc/audit/rules.d/90-space.rules"
echo "  Log:    /var/log/audit/audit.log"
echo "  Search: ausearch -k space-sudo"
echo "  Report: aureport --summary"
