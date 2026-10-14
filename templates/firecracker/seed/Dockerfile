FROM ubuntu:26.04@sha256:da6fc2be547864451aa253836dd926da33623312df4a9a243e35dc877c378a78 AS builder

RUN apt-get update && apt-get install -y \
    squashfs-tools e2fsprogs curl wget ca-certificates gpgv ubuntu-keyring \
    && rm -rf /var/lib/apt/lists/*

COPY rootfs-builder.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/rootfs-builder.sh

# The recipe's dated Ubuntu cloud image as a Kind builder cache directory
# (rootfs-builder.sh's /cache with SPACE_SEED_REUSE_SIGNED_CHECKSUMS=1): the
# signed checksums, then the image they list, each verified by the builder's
# own code. The image's layer is keyed by
# the release and by the signed checksums it was verified against.
# infra/space/kind/scripts/build-seed.sh exports this stage so a Kind seed
# build downloads nothing the build cache already holds.
FROM builder AS cloud-image-checksums
ARG SPACE_SEED_UBUNTU_RELEASE
ARG SPACE_SEED_UBUNTU_VERSION
ARG SPACE_SEED_UBUNTU_DATE
RUN mkdir /cloud-image && SPACE_SEED_REUSE_SIGNED_CHECKSUMS=1 bash -c 'source /usr/local/bin/rootfs-builder.sh \
    && seed_image_source "$SPACE_SEED_UBUNTU_RELEASE" "$SPACE_SEED_UBUNTU_VERSION" "$SPACE_SEED_UBUNTU_DATE" "$(uname -m)" \
    && verified_checksum "$SEED_IMAGE_BASE_URL" "$SEED_IMAGE_NAME" /cloud-image "$SEED_IMAGE_KEYRING" "$SEED_IMAGE_SIGNER"'

FROM builder AS cloud-image-fetch
ARG SPACE_SEED_UBUNTU_RELEASE
ARG SPACE_SEED_UBUNTU_VERSION
ARG SPACE_SEED_UBUNTU_DATE
COPY --from=cloud-image-checksums /cloud-image/ /cloud-image/
RUN SPACE_SEED_REUSE_SIGNED_CHECKSUMS=1 bash -c 'source /usr/local/bin/rootfs-builder.sh \
    && seed_image_source "$SPACE_SEED_UBUNTU_RELEASE" "$SPACE_SEED_UBUNTU_VERSION" "$SPACE_SEED_UBUNTU_DATE" "$(uname -m)" \
    && fetch_verified_squashfs "$SEED_IMAGE_BASE_URL" "$SEED_IMAGE_NAME" /cloud-image "$SEED_IMAGE_KEYRING" "$SEED_IMAGE_SIGNER" ""'

FROM scratch AS cloud-image
COPY --from=cloud-image-fetch /cloud-image/ /

FROM builder

ENTRYPOINT ["rootfs-builder.sh"]
