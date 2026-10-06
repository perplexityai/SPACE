FROM ubuntu:24.04@sha256:008173c23f95b170204355c12626cb5a965d779a7e1283b09e9cffbb1bf33ca3 AS base

ARG UBUNTU_SNAPSHOT=20260923T000000Z

# Ubuntu's minimal image needs a CA bundle before APT can use snapshot HTTPS.
ADD --checksum=sha256:f66dff1bdf8f96060b8177976f8b7d9254bc89bc4db933d769f7384d28480bc9 \
    https://curl.se/ca/cacert-2026-08-13.pem /etc/ssl/certs/ca-certificates.crt
RUN chmod 0644 /etc/ssl/certs/ca-certificates.crt \
    && sed -i "s|^URIs:.*|URIs: https://snapshot.ubuntu.com/ubuntu/${UBUNTU_SNAPSHOT}/|" \
        /etc/apt/sources.list.d/ubuntu.sources

FROM base AS build

ARG KERNEL_VERSION
ARG FIRECRACKER_CI_VERSION
ARG KERNEL_SOURCE_COMMIT
ARG BASE_CONFIG_SHA256
ARG CONFIG_FRAGMENT

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
