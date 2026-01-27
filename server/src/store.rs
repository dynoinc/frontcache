use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use object_store::{
    Error as ObjectStoreError, ObjectStore as ObjStore, ObjectStoreExt, aws::AmazonS3Builder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path as ObjPath,
};
use opentelemetry::KeyValue;
use thiserror::Error;

use crate::prelude::*;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Invalid key format: {0}")]
    InvalidKey(String),
    #[error("Unsupported provider: {0}")]
    UnsupportedProvider(String),
    #[error("Backend error")]
    Backend(#[from] ObjectStoreError),
}

pub struct Store {
    backends: DashMap<(String, String), Arc<dyn ObjStore>>,
    local_root: Option<PathBuf>,
}

impl Store {
    pub fn new(local_root: Option<PathBuf>) -> Self {
        Self {
            backends: DashMap::new(),
            local_root,
        }
    }

    fn parse_key(key: &str) -> Result<(String, String, String), StoreError> {
        let parts: Vec<&str> = key.splitn(2, "://").collect();
        if parts.len() != 2 {
            return Err(StoreError::InvalidKey(key.to_string()));
        }

        let provider = parts[0];
        let rest = parts[1];

        if provider == "file" {
            return Ok((provider.to_string(), String::new(), rest.to_string()));
        }

        let slash_pos = rest
            .find('/')
            .ok_or_else(|| StoreError::InvalidKey(key.to_string()))?;

        let bucket = &rest[..slash_pos];
        let path = &rest[slash_pos + 1..];

        Ok((provider.to_string(), bucket.to_string(), path.to_string()))
    }

    async fn get_backend(
        &self,
        provider: &str,
        bucket: &str,
    ) -> Result<Arc<dyn ObjStore>, StoreError> {
        let cache_key = (provider.to_string(), bucket.to_string());

        if let Some(backend) = self.backends.get(&cache_key) {
            return Ok(backend.clone());
        }

        let backend: Arc<dyn ObjStore> = match provider {
            "s3" => {
                let s3 = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()?;
                Arc::new(s3)
            }
            "gs" => {
                let gcs = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()?;
                Arc::new(gcs)
            }
            "file" => {
                let root = self.local_root.as_ref().ok_or_else(|| {
                    StoreError::UnsupportedProvider("file (--local-root not set)".into())
                })?;
                Arc::new(LocalFileSystem::new_with_prefix(root)?)
            }
            _ => return Err(StoreError::UnsupportedProvider(provider.to_string())),
        };

        self.backends.insert(cache_key, backend.clone());
        Ok(backend)
    }

    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, StoreError> {
        let start = Instant::now();
        let (provider, bucket, path) = Self::parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);
        let range = offset..(offset + length);
        let result = backend.get_range(&obj_path, range).await;

        let m = frontcache_metrics::get();
        let status = result.as_ref().map(|_| "ok").unwrap_or_else(|e| {
            if matches!(e, ObjectStoreError::NotFound { .. }) {
                "not_found"
            } else {
                "error"
            }
        });
        m.store_read_duration.record(
            start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("provider", provider.clone()),
                KeyValue::new("status", status),
            ],
        );

        if let Ok(ref bytes) = result {
            m.store_read_bytes
                .record(bytes.len() as f64, &[KeyValue::new("provider", provider)]);
        }

        result.map_err(Into::into)
    }
}
