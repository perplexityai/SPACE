set -eu
rm -f /etc/systemd/system/space.target.wants/space-apps.target /etc/systemd/system/space-apps.target
# This namespace belongs to native runtime compilation, not arbitrary services.
rm -f /etc/systemd/system/space-runtime@*.service
rm -rf /etc/space/apps
