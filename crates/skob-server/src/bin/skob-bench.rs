use std::process::ExitCode;

use skob_server::cluster::benchmark;

#[tokio::main]
async fn main() -> ExitCode {
    let writers = std::env::var("SKOB_BENCH_WRITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(3);
    let readers = std::env::var("SKOB_BENCH_READERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2);
    let objects = std::env::var("SKOB_BENCH_OBJECTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    match benchmark(writers, readers, objects).await {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap_or_default());
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("skob-bench: {err}");
            ExitCode::from(1)
        }
    }
}
