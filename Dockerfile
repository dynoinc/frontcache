FROM ubuntu AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY frontcache-server /usr/local/bin/frontcache-server
COPY frontcache-router /usr/local/bin/frontcache-router

# dev target adds loadgen for local testing
FROM base AS dev
COPY frontcache-loadgen /usr/local/bin/frontcache-loadgen

# default target (used by CI): server + router only
FROM base
