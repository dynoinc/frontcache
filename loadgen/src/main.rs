use anyhow::Result;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use clap::Parser;
use std::time::Duration;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "frontcache-loadgen")]
struct Args {
    #[arg(long, env = "ROUTER", default_value = "http://localhost:8081")]
    router: String,

    #[arg(long, env = "FILE_COUNT", default_value_t = 10)]
    file_count: u64,

    #[arg(long, env = "FILE_SIZE_MB", default_value_t = 50)]
    file_size_mb: u64,

    #[arg(long, env = "SLEEP_MS", default_value_t = 100)]
    sleep_ms: u64,

    #[arg(long, env = "BUCKET", default_value = "test")]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();
    let file_size = (args.file_size_mb * 1024 * 1024) as usize;
    let sleep_dur = Duration::from_millis(args.sleep_ms);

    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "static");
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&args.router)
        .region(Region::new("us-east-1"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(config);

    let keys: Vec<String> = (1..=args.file_count)
        .map(|i| format!("file{i}.bin"))
        .collect();

    tracing::info!(router = %args.router, files = keys.len(), file_size, "Seeding files via router");
    for key in &keys {
        let data = Bytes::from(random_bytes(file_size));
        loop {
            match client
                .put_object()
                .bucket(&args.bucket)
                .key(key)
                .body(ByteStream::from(data.clone()))
                .send()
                .await
            {
                Ok(_) => {
                    tracing::info!("Seeded {key}");
                    break;
                }
                Err(e) => {
                    tracing::info!("Waiting for router... ({e})");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    tracing::info!("Seeding complete, starting reads");

    let mut count = 0u64;
    loop {
        let idx = fastrand::usize(..keys.len());
        let key = &keys[idx];

        let result = if fastrand::u8(..) < 200 {
            let offset = fastrand::u64(..(file_size as u64 / 4096)) * 4096;
            read_range(&client, &args.bucket, key, offset).await
        } else {
            head_object(&client, &args.bucket, key).await
        };

        match result {
            Ok(_) => {
                count += 1;
                if count.is_multiple_of(100) {
                    println!("Completed {count} reads");
                }
            }
            Err(e) => tracing::warn!("Error on {key}: {e}"),
        }

        tokio::time::sleep(sleep_dur).await;
    }
}

async fn read_range(client: &Client, bucket: &str, key: &str, offset: u64) -> Result<()> {
    let range = format!("bytes={}-{}", offset, offset + 4095);
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(range)
        .send()
        .await?;
    resp.body.collect().await?;
    Ok(())
}

async fn head_object(client: &Client, bucket: &str, key: &str) -> Result<()> {
    client.head_object().bucket(bucket).key(key).send().await?;
    Ok(())
}

fn random_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|_| fastrand::u8(..)).collect()
}
