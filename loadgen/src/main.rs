use anyhow::Result;
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
    let files: Vec<String> = (1..=args.file_count)
        .map(|i| format!("/{}/file{i}.bin", args.bucket))
        .collect();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;

    tracing::info!(router = %args.router, "Waiting for router...");
    loop {
        match client.get(format!("{}/healthz", args.router)).send().await {
            Ok(r) if r.status().is_success() => break,
            _ => tokio::time::sleep(Duration::from_secs(2)).await,
        }
    }

    tracing::info!(files = files.len(), file_size, "Seeding files via router");
    for key in &files {
        let url = format!("{}{}", args.router, key);
        let data: Vec<u8> = (0..file_size).map(|_| fastrand::u8(..)).collect();
        client
            .put(&url)
            .body(data)
            .send()
            .await?
            .error_for_status()?;
        tracing::info!("Seeded {}", key);
    }
    tracing::info!("Seeding complete, starting reads");

    let mut count = 0u64;
    loop {
        let idx = fastrand::usize(..files.len());
        let key = &files[idx];

        // Mix of GET (range read) and HEAD requests
        let result = if fastrand::u8(..) < 200 {
            let offset = fastrand::u64(..(file_size as u64 / 4096)) * 4096;
            read_range(&client, &args.router, key, offset).await
        } else {
            head_object(&client, &args.router, key).await
        };

        match result {
            Ok(_) => {
                count += 1;
                if count.is_multiple_of(100) {
                    println!("Completed {count} reads");
                }
            }
            Err(e) => tracing::warn!("Error on {}: {}", key, e),
        }

        tokio::time::sleep(sleep_dur).await;
    }
}

async fn read_range(
    client: &reqwest::Client,
    router: &str,
    key: &str,
    offset: u64,
) -> Result<usize> {
    let url = format!("{}{}", router, key);
    let resp = client
        .get(&url)
        .header("Range", format!("bytes={}-{}", offset, offset + 4095))
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.bytes().await?.len())
}

async fn head_object(client: &reqwest::Client, router: &str, key: &str) -> Result<usize> {
    let url = format!("{}{}", router, key);
    let resp = client.head(&url).send().await?.error_for_status()?;
    let size = resp
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    Ok(size)
}
