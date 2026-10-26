# syntax=docker/dockerfile:1.7
FROM ubuntu:24.04@sha256:a853f94d226358a79c740cfc7bce0c289748f3fe3488d921d038ccd752c61b60 AS packages

ARG TARGETARCH
ENV DEBIAN_FRONTEND=noninteractive
ENV KERNEL_RELEASE=6.8.0-147-generic
ENV NVIDIA_VERSION=580.178.04

# Kernel and precompiled modules come from the same distribution ABI. Do not
# substitute DKMS: a locally generated key is not trusted by this kernel.
RUN test "$TARGETARCH" = amd64 \
    && printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d \
    && chmod 755 /usr/sbin/policy-rc.d \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        linux-image-6.8.0-147-generic=6.8.0-147.147 \
        linux-modules-nvidia-580-open-6.8.0-147-generic=6.8.0-147.147+1 \
        nvidia-firmware-580-580.178.04=580.178.04-0ubuntu0.24.04.1 \
        initramfs-tools kmod python3 \
    && rm -rf /var/lib/apt/lists/*

FROM packages AS build
COPY build-bundle.sh /build-bundle.sh
RUN bash /build-bundle.sh

FROM scratch AS export
COPY --from=build /output/ /
