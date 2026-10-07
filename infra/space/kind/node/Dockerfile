# Kind node image for SPACE sandbox workers and builders: the stand-in for the
# SPACE node image a production worker or builder boots (infra/space/packer).
#
# Packer bakes the node layer into the image: packages, scripts and enabled
# systemd units, so a node boots with its btrfs /space/cache, cgroup subtree,
# loop-device headroom and /space skeleton before kubelet starts and nothing is
# installed when the node joins. This image does the same for a kind node. The
# units run on every node start, including after `docker restart`, which gives
# the container a fresh /dev, mount table and cgroups; see space-kind-node.sh.
#
# Built and distributed by infra/space/kind/scripts/kind-node-image.sh, which
# tags it by the SHA-256 of exactly the files copied below plus this one. The
# build context is those files alone, so nothing else can reach the image.
#
# The base is the node image kind v0.30.0 (install-prereqs.sh) defaults to,
# pinned by its multi-platform index. The control-plane and infra nodes run the
# same base unchanged (kind-cluster.yaml.tmpl), so every node runs one
# Kubernetes version.
FROM kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a

# mkfs.btrfs for space-cache.service's first format (01-partitions.sh installs
# the same package into the production image). Everything else the units use
# (losetup, blkid, findmnt, mount, install, stat) is in the base image.
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends btrfs-progs \
 && rm -rf /var/lib/apt/lists/* \
 && command -v mkfs.btrfs losetup blkid findmnt >/dev/null

COPY --chmod=0755 infra/space/kind/node/space-kind-node.sh /usr/local/lib/space/kind-node/space-kind-node.sh
COPY --chmod=0644 scripts/lib/space-loop-devices.sh /usr/local/lib/space/kind-node/space-loop-devices.sh
COPY --chmod=0644 infra/space/identity/node-identities.sh /usr/local/lib/space/kind-node/node-identities.sh
COPY --chmod=0644 infra/space/kind/node/systemd/ /etc/systemd/system/
RUN systemctl enable \
      space-loop-nodes.service \
      space-kind-devices.service \
      space-cgroup.service \
      space-layout.service \
      space-cache.service

# The inputs tag, so a node can be matched against the tree that should have
# built it (scripts/check-kind-node-image.sh). Containers inherit image labels.
ARG SPACE_KIND_NODE_IMAGE_TAG=unset
LABEL org.opencontainers.image.version="${SPACE_KIND_NODE_IMAGE_TAG}" \
      org.opencontainers.image.title="space-kind-node"
