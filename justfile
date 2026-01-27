# Run all checks, builds, and tests
check:
    cargo fmt --all
    cargo check --all-targets --all-features
    cargo clippy --all-targets --all-features -- -D warnings
    cargo build --all-targets --all-features
    cargo test --all-targets --all-features
    cargo outdated -R
    cargo audit
