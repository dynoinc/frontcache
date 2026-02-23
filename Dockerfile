FROM ubuntu

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    openssl \
    iproute2 \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY frontcache-server /usr/local/bin/frontcache-server
COPY frontcache-router /usr/local/bin/frontcache-router
COPY wheels/ /tmp/wheels/
RUN pip3 install --no-cache-dir --break-system-packages /tmp/wheels/*.whl && rm -rf /tmp/wheels
