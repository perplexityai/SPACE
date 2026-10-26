# syntax=docker/dockerfile:1.7
FROM rust:1.99.1-bookworm@sha256:48b7f435b8fc6378ce43b44f15e3a47fd8919b2825f835c39bd4bd3da50af5b8 AS build

ARG TARGETARCH
ENV SOURCE_SHA256=000b1194ce4fcc05c496854de96a2b911b91eea59a5e09250e1790f1d6fd2b1c
RUN test "$TARGETARCH" = amd64 \
    && apt-get update \
    && apt-get install -y --no-install-recommends musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl

WORKDIR /source
RUN curl -fLsS https://github.com/cloud-hypervisor/cloud-hypervisor/archive/refs/tags/v54.0.tar.gz -o /source.tar.gz \
    && printf '%s  /source.tar.gz\n' "$SOURCE_SHA256" | sha256sum --check --strict \
    && tar -xzf /source.tar.gz --strip-components=1
COPY vfio-landlock.patch /vfio-landlock.patch
RUN patch --batch --fuzz=0 -p1 < /vfio-landlock.patch
ENV CC_x86_64_unknown_linux_musl=musl-gcc
RUN cargo build --locked --release --target x86_64-unknown-linux-musl -p cloud-hypervisor
RUN mkdir /output \
    && install -m 755 target/x86_64-unknown-linux-musl/release/cloud-hypervisor /output/cloud-hypervisor \
    && cp -r LICENSES /output/ \
    && printf 'upstream=v54.0\nsource_sha256=%s\n' "$SOURCE_SHA256" > /output/source.txt \
    && sha256sum /vfio-landlock.patch >> /output/source.txt \
    && rustc --version >> /output/source.txt \
    && cd /output && sha256sum cloud-hypervisor > SHA256SUMS

FROM scratch AS export
COPY --from=build /output/ /
