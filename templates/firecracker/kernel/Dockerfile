# The linux/amd64 image, not the multi-platform index: BuildKit keys its cache
# by the manifest it resolves, and the image mirror (ci/mirrored-images.txt)
# serves this manifest, so the source and the mirror share cached layers.
FROM ubuntu:24.04@sha256:a2d43bffc46670d7108bb3c0468c67c5557c4c92e83e1e34cbff1322ad1b94dc AS base

ARG UBUNTU_SNAPSHOT=20261020T200000Z

# Ubuntu's minimal image needs a CA bundle before APT can use snapshot HTTPS.
ADD --checksum=sha256:796f1389bfea13c0ed78f663b765baaff54a508e5483786bd6ece73c5e3e3cbe \
    https://curl.se/ca/cacert-2026-10-20.pem /etc/ssl/certs/ca-certificates.crt
RUN chmod 0644 /etc/ssl/certs/ca-certificates.crt \
    && sed -i "s|^URIs:.*|URIs: https://snapshot.ubuntu.com/ubuntu/${UBUNTU_SNAPSHOT}/|" \
        /etc/apt/sources.list.d/ubuntu.sources

# The build toolchain is its own stage so that CI can cache it without the
# kernel compile (ci.sh).
FROM base AS toolchain

RUN apt-get -o Acquire::Retries=5 update --error-on=any \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=5 install --yes --no-install-recommends \
        bc \
        binutils \
        bison \
        build-essential \
        ca-certificates \
        curl \
        dwarves \
        flex \
        git \
        libelf-dev \
        libssl-dev \
        openssl \
        python3 \
    && rm -rf /var/lib/apt/lists/*

FROM toolchain AS build

# Declared after the toolchain: a RUN depends on every ARG declared before it,
# so a kernel bump would otherwise reinstall the packages.
ARG KERNEL_VERSION
ARG FIRECRACKER_CI_VERSION
ARG KERNEL_SOURCE_COMMIT
ARG BASE_CONFIG_SHA256
ARG CONFIG_FRAGMENT

WORKDIR /build
RUN test -n "${KERNEL_VERSION}" \
    && test -n "${FIRECRACKER_CI_VERSION}" \
    && test -n "${KERNEL_SOURCE_COMMIT}" \
    && test -n "${BASE_CONFIG_SHA256}" \
    && git init linux \
    && git -C linux remote add origin https://github.com/amazonlinux/linux.git \
    && for attempt in 1 2 3 4 5; do \
        if git -C linux fetch --depth 1 origin "${KERNEL_SOURCE_COMMIT}"; then \
          break; \
        fi; \
        if [ "${attempt}" -eq 5 ]; then \
          exit 1; \
        fi; \
        delay=$((attempt * 5)); \
        echo "Kernel source fetch failed (attempt ${attempt}/5); retrying in ${delay}s" >&2; \
        sleep "${delay}"; \
      done \
    && git -C linux checkout --detach FETCH_HEAD \
    && test "$(git -C linux rev-parse HEAD)" = "${KERNEL_SOURCE_COMMIT}" \
    && git -C linux show -s --format=%ct HEAD > source-date-epoch \
    && rm -rf linux/.git

RUN curl --fail --location --retry 5 --output base.config \
        "https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/${FIRECRACKER_CI_VERSION}/x86_64/vmlinux-${KERNEL_VERSION}.config" \
    && echo "${BASE_CONFIG_SHA256}  base.config" | sha256sum --check --strict

ENV KBUILD_BUILD_USER=space \
    KBUILD_BUILD_HOST=kernel-builder \
    KERNEL_VERSION=${KERNEL_VERSION}

COPY build-kernel.sh ./
# Optional caller-supplied fragments stay in the input context, not the image.
RUN --mount=type=bind,target=/inputs \
    CONFIG_FRAGMENT="${CONFIG_FRAGMENT:+/inputs/${CONFIG_FRAGMENT}}" ./build-kernel.sh

FROM scratch AS export
COPY --from=build /output/ /

FROM base AS smoke

RUN apt-get -o Acquire::Retries=5 update --error-on=any \
    && DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=5 install --yes --no-install-recommends \
        qemu-system-x86 \
    && rm -rf /var/lib/apt/lists/*
