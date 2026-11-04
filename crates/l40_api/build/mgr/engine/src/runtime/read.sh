set -eu
if [ -e /etc/space/apps ] || [ -L /etc/space/apps ]; then
    test ! -L /etc/space/apps
    test -d /etc/space/apps
    for unit in /etc/space/apps/*.service /etc/space/apps/*.target; do
        test "$(readlink "/etc/systemd/system/${unit##*/}")" = "$unit"
    done
    test "$(readlink /etc/systemd/system/space.target.wants/space-apps.target)" = /etc/space/apps/space-apps.target
    for unit in /etc/systemd/system/space-runtime@*.service; do
        test -f "/etc/space/apps/${unit##*/}"
    done
    tar -cf - -C /etc/space/apps .
else
    for path in /etc/systemd/system/space-runtime@*.service /etc/systemd/system/space-apps.target /etc/systemd/system/space.target.wants/space-apps.target; do
        if [ -e "$path" ] || [ -L "$path" ]; then
            echo 'orphaned native startup definitions; use the native recipe operations' >&2
            exit 1
        fi
    done
fi
