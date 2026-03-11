use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct BucketEntry {
    pub provider: String,
}

fn default_provider() -> String {
    "s3".into()
}

#[derive(Deserialize)]
pub struct BucketConfig {
    #[serde(default = "default_provider")]
    pub default_provider: String,
    #[serde(default)]
    pub buckets: HashMap<String, BucketEntry>,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self {
            default_provider: default_provider(),
            buckets: HashMap::new(),
        }
    }
}

impl BucketConfig {
    pub fn load(path: Option<&Path>) -> Result<Self> {
        match path {
            Some(p) => {
                let content = std::fs::read_to_string(p)?;
                Ok(serde_yaml::from_str(&content)?)
            }
            None => Ok(Self::default()),
        }
    }

    pub fn provider_for(&self, bucket: &str) -> &str {
        self.buckets
            .get(bucket)
            .map(|e| e.provider.as_str())
            .unwrap_or(&self.default_provider)
    }
}
