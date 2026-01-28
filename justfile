# Run all checks and tests
check:
    cargo fmt --all
    cargo check --all-targets --all-features
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test --all-targets --all-features
