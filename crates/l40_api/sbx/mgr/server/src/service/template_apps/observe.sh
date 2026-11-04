for program in timeout systemctl sleep; do
    command -v "$program" >/dev/null 2>&1 || exit 127
done
while :; do
    timeout 2s systemctl show --no-pager -p Id -p LoadState -p ActiveState -p SubState -p InactiveExitTimestampMonotonic -- "$@" 2>/dev/null || true
    printf '\n.\n'
    sleep 0.3 || exit 127
done
