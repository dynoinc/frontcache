use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::DashMap;
use object_store::memory::InMemory;
use object_store::{
    Error as ObjectStoreError, GetOptions, MultipartUpload, ObjectStore as ObjStore,
    ObjectStoreExt, PutMultipartOptions, PutPayload, PutResult, aws::AmazonS3Builder,
    gcp::GoogleCloudStorageBuilder, path::Path as ObjPath,
};
use opentelemetry::KeyValue;
use thiserror::Error;

use crate::config::BucketConfig;

const VERSION_HEADER: &str = "x-frontcache-version";

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("Invalid key format: {0}")]
    InvalidKey(String),
    #[error("Invalid byte range: offset={offset}, length={length}")]
    InvalidRange { offset: u64, length: u64 },
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
    pub object_size: u64,
}

pub struct Store {
    pub backends: DashMap<(String, String), Arc<dyn ObjStore>>,
    config: Arc<BucketConfig>,
}

impl Store {
    pub fn new(config: Arc<BucketConfig>) -> Self {
        Self {
            backends: DashMap::new(),
            config,
        }
    }

    /// Parse key from `/<bucket>/<path>` format.
    /// Returns (provider, bucket, path).
    fn parse_key(&self, key: &str) -> Result<(String, String, String), StoreError> {
        let err = || StoreError::InvalidKey(key.to_string());
        let stripped = key.strip_prefix('/').ok_or_else(err)?;
        let (bucket, path) = stripped.split_once('/').ok_or_else(err)?;
        let provider = self.config.provider_for(bucket).to_string();
        Ok((provider, bucket.to_string(), path.to_string()))
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
            "inmem" => Arc::new(InMemory::new()),
            _ => return Err(StoreError::UnsupportedProvider(provider.to_string())),
        };

        self.backends.insert(cache_key, backend.clone());
        Ok(backend)
    }

    fn status_label<T>(result: &Result<T, StoreError>) -> &'static str {
        match result {
            Ok(_) => "ok",
            Err(StoreError::NotFound(_)) => "not_found",
            Err(_) => "error",
        }
    }

    fn provider_label(provider: &str) -> &'static str {
        match provider {
            "s3" => "s3",
            "gs" => "gs",
            "inmem" => "inmem",
            _ => "unknown",
        }
    }

    pub async fn head(&self, key: &str) -> Result<String, StoreError> {
        let start = Instant::now();
        let (provider, bucket, path) = self.parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);

        let opts = GetOptions {
            head: true,
            ..Default::default()
        };
        let result = backend
            .get_opts(&obj_path, opts)
            .await
            .map_err(|e| StoreError::from_object_store(e, key));

        let m = frontcache_metrics::get();
        m.store_duration.record(
            start.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("provider", provider),
                KeyValue::new("status", Self::status_label(&result)),
                KeyValue::new("operation", "head"),
            ],
        );

        Ok(Self::extract_version(&result?))
    }

    fn extract_version(result: &object_store::GetResult) -> String {
        if let Some(version) = result
            .attributes
            .get(&object_store::Attribute::Metadata(VERSION_HEADER.into()))
        {
            version.to_string().trim_matches('"').to_string()
        } else {
            result
                .meta
                .e_tag
                .as_deref()
                .unwrap_or_default()
                .trim_matches('"')
                .to_string()
        }
    }

    pub async fn read_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<ReadResult, StoreError> {
        let start = Instant::now();
        let (provider, bucket, path) = self.parse_key(key)?;
        let provider_label = Self::provider_label(&provider);
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);
        let end = offset
            .checked_add(length)
            .ok_or(StoreError::InvalidRange { offset, length })?;

        let opts = GetOptions {
            range: Some((offset..end).into()),
            ..Default::default()
        };
        let result = backend.get_opts(&obj_path, opts).await;
        let result = result.map_err(|e| StoreError::from_object_store(e, key));

        let m = frontcache_metrics::get();
        m.store_duration.record(
            start.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("provider", provider_label),
                KeyValue::new("status", Self::status_label(&result)),
                KeyValue::new("operation", "read"),
            ],
        );

        let get_result = result?;
        let version = Self::extract_version(&get_result);
        let object_size = get_result.meta.size;

        let data = get_result
            .bytes()
            .await
            .map_err(|e| StoreError::from_object_store(e, key))?;

        m.store_read_bytes.record(
            data.len() as f64,
            &[KeyValue::new("provider", provider_label)],
        );

        Ok(ReadResult {
            data,
            version,
            object_size,
        })
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<PutResult, StoreError> {
        let (provider, bucket, path) = self.parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);
        backend
            .put(&obj_path, PutPayload::from(data))
            .await
            .map_err(|e| StoreError::from_object_store(e, key))
    }

    pub async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let (provider, bucket, path) = self.parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);
        backend
            .delete(&obj_path)
            .await
            .map_err(|e| StoreError::from_object_store(e, key))
    }

    pub async fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<object_store::ListResult, StoreError> {
        let provider = self.config.provider_for(bucket).to_string();
        let backend = self.get_backend(&provider, bucket).await?;
        let obj_prefix = prefix.map(ObjPath::from);
        backend
            .list_with_delimiter(obj_prefix.as_ref())
            .await
            .map_err(StoreError::Backend)
    }

    pub async fn create_multipart(
        &self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload>, StoreError> {
        let (provider, bucket, path) = self.parse_key(key)?;
        let backend = self.get_backend(&provider, &bucket).await?;
        let obj_path = ObjPath::from(path);
        backend
            .put_multipart_opts(&obj_path, PutMultipartOptions::default())
            .await
            .map_err(|e| StoreError::from_object_store(e, key))
    }
}
