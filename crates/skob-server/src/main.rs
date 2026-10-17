use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use clap::Subcommand;
use skob_core::Role;
use skob_core::SkobError;
use skob_server::build;
use skob_server::serve;
use skob_server::ServeConfig;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "skob", about = "Scalable blob database")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the read plane, the write plane, or both.
    Serve(ServeArgs),
}

#[derive(clap::Args)]
struct ServeArgs {
    /// `all`, `reader`, or `writer`.
    #[arg(long, env = "SKOB_ROLE", default_value = "all")]
    role: String,

    /// Listen address.
    #[arg(long, env = "SKOB_BIND", default_value = "0.0.0.0:8080")]
    bind: String,

    /// Local cache and Raft directory.
    #[arg(long, env = "SKOB_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// Raft node id.
    #[arg(long, env = "SKOB_NODE_ID", default_value_t = 1)]
    node_id: u64,

    /// Address other writers would use to reach this node.
    #[arg(long, env = "SKOB_ADVERTISE", default_value = "http://127.0.0.1:8080")]
    advertise: String,

    /// `memory`, `fs`, or `s3`.
    #[arg(long, env = "SKOB_BLOB", default_value = "fs")]
    blob: String,

    /// S3-compatible endpoint. Unset for AWS.
    #[arg(long, env = "SKOB_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    #[arg(long, env = "SKOB_S3_BUCKET", default_value = "skob")]
    s3_bucket: String,

    #[arg(long, env = "SKOB_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    #[arg(long, env = "SKOB_S3_ACCESS_KEY")]
    s3_access_key: Option<String>,

    #[arg(long, env = "SKOB_S3_SECRET_KEY")]
    s3_secret_key: Option<String>,

    /// Path-style S3 addressing. Required for RustFS and MinIO.
    #[arg(long, env = "SKOB_S3_PATH_STYLE", default_value_t = false)]
    s3_path_style: bool,

    #[arg(long, env = "SKOB_SEGMENT_COUNT", default_value_t = 256)]
    segment_count: u32,

    /// PostgreSQL wire listen address. The database name is the project id.
    #[arg(long, env = "SKOB_PG_BIND", default_value = "0.0.0.0:5432")]
    pg_bind: String,

    /// Do not listen for PostgreSQL clients.
    #[arg(long, env = "SKOB_PG_DISABLE", default_value_t = false)]
    pg_disable: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                EnvFilter::new("skob=info,skob_server=info,skob_raft=info,skob_index=info,skob_storage=info")
            }),
        )
        .init();

    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        eprintln!("skob: {err}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

async fn run(cli: Cli) -> skob_core::Result<()> {
    let Command::Serve(args) = cli.command;
    let role = Role::parse(&args.role)?;
    if args.segment_count == 0 {
        return Err(SkobError::BadRequest("segment_count must be non-zero".into()));
    }
    let state = build(ServeConfig {
        role,
        bind: args.bind.clone(),
        data_dir: args.data_dir,
        node_id: args.node_id,
        advertise: args.advertise,
        blob: args.blob,
        s3_endpoint: args.s3_endpoint,
        s3_bucket: args.s3_bucket,
        s3_region: args.s3_region,
        s3_access_key: args.s3_access_key,
        s3_secret_key: args.s3_secret_key,
        s3_path_style: args.s3_path_style,
        segment_count: args.segment_count,
    })
    .await?;
    let pg_bind = (!args.pg_disable && !args.pg_bind.is_empty()).then_some(args.pg_bind.as_str());
    serve(state, &args.bind, pg_bind).await
}
