use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use object_store::{
    Error as ObjectStoreError, GetOptions, ObjectStore as ObjStore, aws::AmazonS3Builder,
    gcp::GoogleCloudStorageBuilder, path::Path as ObjPath,
};
use opentelemetry::KeyValue;
use thiserror::Error;

use std::sync::Arc;

const VERSION_HEADER: &str = "x-frontcache-version";

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Invalid key format: {0}")]
    InvalidKey(String),
    #[error("Unsupported provider: {0}")]
    UnsupportedProvider(String),
    #[error("Object not found: {0}")]
    NotFound(String),
    #[error("Backend error: {0}")]
    Backend(ObjectStoreError),
}

impl StoreError {
    fn from_object_store(e: ObjectStoreError, path: &str) -> Self {
        match e {
            ObjectStoreError::NotFound { .. } => StoreError::NotFound(path.to_string()),
            e => StoreError::Backend(e),
        }
    }
}

pub struct ReadResult {
    pub data: Bytes,
    pub version: String,
}

pub struct Store {
    backends: DashMap<(String, String), Arc<dyn ObjStore>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            backends: DashMap::new(),
        }
    }

    fn parse_key(key: &str) -> Result<(String, String, String), StoreError> {
        let parts: Vec<&str> = key.splitn(2, "://").collect();
        if parts.len() != 2 {
            return Err(StoreError::InvalidKey(key.to_string()));
        }

        let provider = parts[0];
        let rest = parts[1];

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
                    .build()
                    .map_err(StoreError::Backend)?;
                Arc::new(s3)
            }
            "gs" => {
                let gcs = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(StoreError::Backend)?;
                Arc::new(gcs)
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
    ) -> Result<ReadResult, StoreError> {
        let start = Instant::now();
        let (provider, bucket, path) = Self::parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);

        let opts = GetOptions {
            range: Some((offset..(offset + length)).into()),
            ..Default::default()
        };
        let result = backend.get_opts(&obj_path, opts).await;

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

        let get_result = result.map_err(|e| StoreError::from_object_store(e, key))?;
        let version = get_result
            .attributes
            .get(&object_store::Attribute::Metadata(VERSION_HEADER.into()))
            .map(|v| v.to_string())
            .unwrap_or_else(|| get_result.meta.e_tag.clone().unwrap_or_default())
            .trim_matches('"')
            .to_string();

        let data = get_result
            .bytes()
            .await
            .map_err(|e| StoreError::from_object_store(e, key))?;

        m.store_read_bytes
            .record(data.len() as f64, &[KeyValue::new("provider", provider)]);

        Ok(ReadResult { data, version })
    }
}
