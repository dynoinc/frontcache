FROM ubuntu

RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY frontcache-server /usr/local/bin/frontcache-server
COPY frontcache-router /usr/local/bin/frontcache-router
