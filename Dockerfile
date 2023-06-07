FROM golang:1.18.3-buster as builder

# Install deps
RUN apt-get update && apt-get install -y \
  libssl-dev \
  ca-certificates \
  fuse

ENV SRC_DIR /build

# Download packages first so they can be cached.
COPY go.mod go.sum $SRC_DIR/
RUN cd $SRC_DIR \
  && go mod download

COPY . $SRC_DIR

RUN cd $SRC_DIR/cmd/cas \
  && CGO_ENABLED=0 GOOS=linux GOTAGS=openssl go build -a -o cas .

# Get tini, a very minimal init daemon for containers.
ENV TINI_VERSION v0.19.0
RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
        "amd64" | "armhf" | "arm64") tiniArch="tini-static-$dpkgArch" ;;\
        *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
  cd /tmp \
  && wget -q -O tini https://github.com/krallin/tini/releases/download/$TINI_VERSION/$tiniArch \
  && chmod +x tini

# Now comes the actual target image, which aims to be as small as possible.
FROM busybox:1.31.1-glibc as target

ARG ENV_TAG
ENV ENV_TAG=$ENV_TAG

COPY --from=builder /tmp/tini /sbin/tini
COPY --from=builder /etc/ssl/certs /etc/ssl/certs

# This shared lib (part of glibc) doesn't seem to be included with busybox.
COPY --from=builder /lib/*-linux-gnu*/libdl.so.2 /lib/

# Copy over SSL libraries.
COPY --from=builder /usr/lib/*-linux-gnu*/libssl.so* /usr/lib/
COPY --from=builder /usr/lib/*-linux-gnu*/libcrypto.so* /usr/lib/

# Get the CAS binary and environment variables from the build container.
ENV SRC_DIR /build
COPY --from=builder $SRC_DIR/env/* /usr/local/bin/cas/env/
COPY --from=builder $SRC_DIR/cmd/cas/cas /usr/local/bin/cas/cas

EXPOSE 8080

WORKDIR /usr/local/bin/cas

CMD ["/sbin/tini", "-s", "./cas"]
