# Run all checks and tests
check:
    cargo fmt --all
    cargo check --all-targets --all-features
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets --all-features

# Build docker image for local k8s testing (debug, cross-compiled for Linux)
docker-build:
    cargo zigbuild --target aarch64-unknown-linux-gnu -p frontcache-server -p frontcache-router -p frontcache-loadgen
    docker build --target dev -f Dockerfile -t localhost/frontcache:dev target/aarch64-unknown-linux-gnu/debug/
