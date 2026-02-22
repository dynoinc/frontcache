# Run all checks and tests
check:
    protoc \
        --proto_path=proto \
        --include_imports \
        --include_source_info \
        --descriptor_set_out=proto/cache_descriptor_set.bin \
        proto/cache.proto

    cargo fmt --all
    cargo check --all-targets --all-features
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets --all-features
    cd py && uv run maturin build

# Build docker image for local k8s testing (debug, cross-compiled for Linux)
docker-build:
    cargo zigbuild --target aarch64-unknown-linux-gnu -p frontcache-server -p frontcache-router
    docker build -f Dockerfile -t frontcache:dev target/aarch64-unknown-linux-gnu/debug/
