set -eu
test ! -L /etc/space
mkdir -p /etc/space
stage=$(mktemp -d /etc/space/.apps-build.XXXXXX)
trap 'rm -rf "$stage"' EXIT
# Input is the builder's own regular-file archive, never a tenant archive.
tar -xf - -C "$stage"
rm -f /etc/systemd/system/space-runtime@*.service
rm -rf /etc/space/apps
mv "$stage" /etc/space/apps
mkdir -p /etc/systemd/system/space.target.wants
for unit in /etc/space/apps/*.service /etc/space/apps/*.target; do
    ln -sfn "$unit" "/etc/systemd/system/${unit##*/}"
done
ln -sfn /etc/space/apps/space-apps.target /etc/systemd/system/space.target.wants/space-apps.target
