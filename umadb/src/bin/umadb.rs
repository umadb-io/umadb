use clap::{CommandFactory, FromArgMatches, Parser};
use umadb::{ReadMethod, RunOptions, print_banner, run_async, spawn_shutdown_on_signal, uptime};

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to bind to
    #[arg(
        long = "listen",
        env = "UMADB_LISTEN",
        default_value = "127.0.0.1:50051"
    )]
    listen: String,

    /// Path to database file or directory
    #[arg(long = "db-path", env = "UMADB_DB_PATH", default_value = "./uma.db")]
    db_path: String,

    /// Path to server TLS certificate (PEM), optional
    #[arg(long = "tls-cert", env = "UMADB_TLS_CERT", required = false)]
    cert_path: Option<String>,

    /// Path to server TLS private key (PEM), optional
    #[arg(long = "tls-key", env = "UMADB_TLS_KEY", required = false)]
    key_path: Option<String>,

    /// API key for authenticating clients, optional
    #[arg(long = "api-key", env = "UMADB_API_KEY", required = false)]
    api_key: Option<String>,

    /// Read method (fileio or mmap)
    #[arg(
        long = "read-method",
        env = "UMADB_READ_METHOD",
        default_value = "fileio"
    )]
    read_method: String,

    /// Page cache max pages (0 to disable)
    #[arg(
        long = "page-cache-max-pages",
        env = "UMADB_PAGE_CACHE_MAX_PAGES",
        default_value = "0"
    )]
    page_cache_max_pages: usize,

    /// Page cache max size in MB (0 to disable)
    #[arg(
        long = "page-cache-max-mb",
        env = "UMADB_PAGE_CACHE_MAX_MB",
        default_value = "0"
    )]
    page_cache_max_mb: usize,

    /// Zero-fill pages
    #[arg(long = "zero-fill-pages", env = "UMADB_ZERO_FILL_PAGES", default_value = "true", action = clap::ArgAction::Set)]
    zero_fill_pages: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = uptime();
    let rt = umadb::build_runtime()?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Args::command();
    cmd = cmd.about(format!(
        "UmaDB gRPC server ({}-{})",
        std::env::consts::OS,
        std::env::consts::ARCH
    ));

    let matches = cmd.get_matches();
    let args = Args::from_arg_matches(&matches)?; // <-- FromArgMatches trait

    let run_options = RunOptions {
        listen_addr: args.listen,
        db_path: args.db_path,
        tls_cert_path: args.cert_path,
        tls_key_path: args.key_path,
        api_key: args.api_key,
        read_method: args.read_method.parse().unwrap_or(ReadMethod::Mmap),
        page_cache_max_pages: args.page_cache_max_pages,
        page_cache_max_mb: args.page_cache_max_mb,
        zero_fill_pages: args.zero_fill_pages,
    };

    if let Err(err) = run_options.validate() {
        eprintln!(
            "Both --tls-cert and --tls-key (or UMADB_TLS_CERT and UMADB_TLS_KEY) must be provided for TLS: {err}"
        );
        std::process::exit(2);
    }

    let (rx, _shutdown_task) = spawn_shutdown_on_signal();

    print_banner();

    run_async(run_options, rx).await?;

    Ok(())
}
