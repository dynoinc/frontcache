use anyhow::Result;
use clap::Parser;
use frontcache_client::CacheClient;
use futures::StreamExt;
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

    #[arg(long, env = "BUCKET", default_value = "s3://test")]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    frontcache_metrics::init();

    let args = Args::parse();
    let file_size = args.file_size_mb * 1024 * 1024;
    let sleep_dur = Duration::from_millis(args.sleep_ms);
    let files: Vec<String> = (1..=args.file_count)
        .map(|i| format!("{}/file{i}.bin", args.bucket))
        .collect();

    tracing::info!(router = %args.router, files = files.len(), file_size, "Connecting");
    let client = CacheClient::new(args.router).await?;
    tracing::info!("Connected");

    let mut count = 0u64;
    loop {
        let idx = fastrand::usize(..files.len());
        let offset = fastrand::u64(..(file_size / 4096)) * 4096;

        match drain(&client, &files[idx], offset).await {
            Ok(_) => {
                count += 1;
                if count.is_multiple_of(100) {
                    println!("Completed {count} reads");
                }
            }
            Err(e) => tracing::warn!("Error reading {} offset={}: {}", files[idx], offset, e),
        }

        tokio::time::sleep(sleep_dur).await;
    }
}

async fn drain(client: &CacheClient, file: &str, offset: u64) -> Result<usize> {
    let mut stream = client.stream_range(file, offset, 4096, Some("v1"))?;
    let mut nbytes = 0;
    while let Some(chunk) = stream.next().await {
        nbytes += chunk?.len();
    }
    Ok(nbytes)
}
