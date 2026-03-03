use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct FetchLimiter {
    concurrency: Option<Arc<Semaphore>>,
    req_rate: Option<DefaultDirectRateLimiter>,
    bandwidth: Option<(DefaultDirectRateLimiter, NonZeroU32)>,
    timeout: Option<Duration>,
}

impl FetchLimiter {
    pub fn from_env() -> Self {
        let make_limiter = |rate: u32, burst: u32| -> Option<DefaultDirectRateLimiter> {
            let r = NonZeroU32::new(rate)?;
            let b = NonZeroU32::new(burst.max(1)).unwrap();
            Some(RateLimiter::direct(Quota::per_second(r).allow_burst(b)))
        };

        let bw_burst: u32 = crate::env_or("FRONTCACHE_STORE_BYTES_BURST", 16u32 * 1024 * 1024);
        let bw_rate: u32 = crate::env_or("FRONTCACHE_STORE_MAX_BYTES_PER_SEC", 0);
        let timeout_ms: u64 = crate::env_or("FRONTCACHE_STORE_LIMIT_WAIT_TIMEOUT_MS", 0u64);

        Self {
            concurrency: NonZeroU32::new(crate::env_or(
                "FRONTCACHE_STORE_MAX_INFLIGHT_FETCHES",
                0u32,
            ))
            .map(|n| Arc::new(Semaphore::new(n.get() as usize))),
            req_rate: make_limiter(
                crate::env_or("FRONTCACHE_STORE_MAX_REQUESTS_PER_SEC", 0),
                crate::env_or("FRONTCACHE_STORE_REQUESTS_BURST", 1),
            ),
            bandwidth: make_limiter(bw_rate, bw_burst)
                .map(|l| (l, NonZeroU32::new(bw_burst.max(1)).unwrap())),
            timeout: (timeout_ms > 0).then(|| Duration::from_millis(timeout_ms)),
        }
    }

    pub async fn acquire_concurrency(&self) -> Result<Option<OwnedSemaphorePermit>, ()> {
        let Some(sem) = &self.concurrency else {
            return Ok(None);
        };
        let sem = sem.clone();
        let fut = async move { sem.acquire_owned().await.unwrap() };
        let permit = if let Some(d) = self.timeout {
            tokio::time::timeout(d, fut).await.map_err(|_| ())?
        } else {
            fut.await
        };
        Ok(Some(permit))
    }

    pub async fn wait_for_request(&self) -> Result<(), ()> {
        let Some(l) = &self.req_rate else {
            return Ok(());
        };
        let fut = l.until_ready();
        if let Some(d) = self.timeout {
            tokio::time::timeout(d, fut).await.map_err(|_| ())
        } else {
            fut.await;
            Ok(())
        }
    }

    pub async fn wait_for_bytes(&self, bytes: usize) -> Result<(), ()> {
        let Some((l, burst)) = &self.bandwidth else {
            return Ok(());
        };
        let Some(n) = NonZeroU32::new((bytes as u32).min(burst.get())) else {
            return Ok(());
        };
        // n <= burst so InsufficientCapacity won't occur; map it to () anyway
        if let Some(d) = self.timeout {
            tokio::time::timeout(d, l.until_n_ready(n))
                .await
                .map_err(|_| ())?
                .map_err(|_| ())
        } else {
            l.until_n_ready(n).await.map_err(|_| ())
        }
    }
}
