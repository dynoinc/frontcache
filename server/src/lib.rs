pub mod block;
pub mod cache;
pub mod disk;
pub mod index;
pub mod limiter;
pub mod server;
pub mod store;

pub(crate) fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    match std::env::var(name) {
        Ok(v) => match v.parse() {
            Ok(parsed) => parsed,
            Err(_) => {
                tracing::warn!("Invalid value '{}' for {}, using default", v, name);
                default
            }
        },
        Err(_) => default,
    }
}
