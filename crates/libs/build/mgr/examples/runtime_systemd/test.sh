#!/bin/bash
# Destructive fixture: only run inside an explicitly marked disposable container.
set -euo pipefail
test -f /run/space-runtime-test-container
# Missing observer utilities must exit, not spin and consume guest CPU.
if PATH=/nonexistent /bin/sh /fixtures/observe.sh; then
    exit 1
else
    test "$?" -eq 127
fi
mkdir -p /etc/systemd/system /run/runtime-observed
chmod 0777 /run/runtime-observed
python3 - <<'PYFIXTURE'
import json, os, shutil
with open('/fixtures/expected.json') as f: expected = json.load(f)
cwd = expected['cwd']
os.makedirs(expected['worker_cwd'], exist_ok=True)
os.makedirs(cwd, exist_ok=True)
shutil.copyfile('/fixtures/capture.py', cwd + '/capture.py')
os.chmod(cwd + '/capture.py', 0o755)
PYFIXTURE
cat > /etc/systemd/system/space.slice <<'UNIT'
[Slice]
MemoryMax=80%
CPUWeight=1
UNIT
cat > /etc/systemd/system/spaced.service <<'UNIT'
[Service]
ExecStart=/bin/sleep infinity
UNIT
cat > /etc/systemd/system/space.target <<'UNIT'
[Unit]
DefaultDependencies=no
Wants=spaced.service
After=spaced.service
UNIT
bash /fixtures/install.sh < /fixtures/apps.tar
systemctl daemon-reload
systemctl start space.target
wait_property() {
    local unit=$1 property=$2 expected=$3
    for ((i=0; i<100; i++)); do
        if [ "$(systemctl show "$unit" --property="$property" --value)" = "$expected" ]; then return; fi
        sleep 0.05
    done
    systemctl status "$unit" --no-pager || true
    return 1
}
wait_property space-runtime@app.service SubState start-post
python3 /fixtures/check-observer.py space-runtime@app.service=activating/start-post
# The platform target completes even while a probe is pending.
systemctl is-active --quiet space.target
touch /run/runtime-ready
wait_property space-runtime@app.service ActiveState active
wait_property space-runtime@worker.service ActiveState active
python3 /fixtures/check-observer.py space-runtime@app.service=active/running space-runtime@worker.service=active/running
python3 - <<'PY'
import json, os, subprocess
with open('/run/runtime-observed/app.json') as f: actual = json.load(f)
with open('/fixtures/expected.json') as f: expected = json.load(f)
assert actual['argv'] == expected['argv'], actual
assert actual['env'] == expected['env'], actual
assert actual['user'] == 65534, actual
assert actual['cwd'] == expected['cwd'], actual
assert actual['setting'] == 'one', actual
worker = subprocess.check_output(['systemctl', 'show', 'space-runtime@worker.service', '-p', 'MainPID', '--value']).decode().strip()
assert os.readlink('/proc/' + worker + '/cwd') == expected['worker_cwd']
PY
test ! -e /run/runtime-injected
pid=$(systemctl show space-runtime@app.service --property=MainPID --value)
worker=$(systemctl show space-runtime@worker.service --property=MainPID --value)
systemctl restart spaced.service
test "$(systemctl show space-runtime@app.service --property=MainPID --value)" = "$pid"
systemctl restart systemd-journald.service
test "$(systemctl show space-runtime@app.service --property=MainPID --value)" = "$pid"
journalctl --sync
journalctl -u space-runtime@app.service --no-pager | grep -q runtime-probe-output
# Tenant unit changes follow normal systemd reload/restart semantics.
sed -i 's/RUNTIME_TEST=one/RUNTIME_TEST=two/' /etc/space/apps/space-runtime@app.service
systemctl daemon-reload
systemctl restart space-runtime@app.service
wait_property space-runtime@app.service ActiveState active
python3 -c 'import json; assert json.load(open("/run/runtime-observed/app.json"))["setting"] == "two"'
test "$(systemctl show space-runtime@worker.service --property=MainPID --value)" = "$worker"
# A failed app never stops the platform target or its sibling.
systemctl kill --kill-whom=main --signal=KILL space-runtime@app.service
wait_property space-runtime@app.service ActiveState active
systemctl is-active --quiet space.target space-runtime@worker.service
# Exhaust an app's real restart budget without affecting its sibling or guest.
sed -i '/^ExecStart=/c\ExecStart=/bin/false' /etc/space/apps/space-runtime@app.service
systemctl daemon-reload
systemctl restart --no-block space-runtime@app.service
# Automatic retry uses activating/auto-restart; failed means retries stopped.
for ((i=0; i<250; i++)); do
    if [ "$(systemctl show space-runtime@app.service -p ActiveState --value)" = failed ]; then break; fi
    sleep 0.05
done
wait_property space-runtime@app.service ActiveState failed
test "$(systemctl show space-runtime@app.service -p MainPID --value)" = 0
test "$(systemctl show space-runtime@app.service -p NRestarts --value)" -gt 0
systemctl is-active --quiet space.target space-runtime@worker.service
systemctl stop 'space-runtime@*.service' space-apps.target
# Emulate builder boot arguments inside this private mount namespace. Both the
# group mask and each app's condition prevent automatic or direct startup.
printf 'systemd.mask=space-apps.target\n' > /run/builder-cmdline
mount --bind /run/builder-cmdline /proc/cmdline
# In a container systemd reads PID1 argv instead of the VM kernel command line.
printf '/sbin/init\0systemd.mask=space-apps.target\0' > /run/builder-pid1-cmdline
mount --bind /run/builder-pid1-cmdline /proc/1/cmdline
systemctl daemon-reload
bash /fixtures/check.sh
systemctl reset-failed space-runtime@app.service
systemctl start space-runtime@app.service
test "$(systemctl show space-runtime@app.service --property=MainPID --value)" = 0
systemctl start space-apps.target && exit 1
# Replacement and clear remove stale owned units and links.
bash /fixtures/install.sh < /fixtures/replacement.tar
test ! -e /etc/systemd/system/space-runtime@app.service
test -e /etc/systemd/system/space-runtime@worker.service
bash /fixtures/read.sh > /run/roundtrip.tar
bash /fixtures/clear.sh
test -z "$(bash /fixtures/read.sh)"
test ! -e /etc/systemd/system/space-runtime@app.service
# Legacy-only templates can remain, but native edits reject legacy startup.
printf '[Service]\nExecStart=/bin/true\n' > /etc/systemd/system/e2b-template-start.service
if bash /fixtures/check.sh; then exit 1; fi
rm /etc/systemd/system/e2b-template-start.service
umount /proc/1/cmdline
umount /proc/cmdline
systemctl daemon-reload
printf 'runtime unit acceptance passed\n'
