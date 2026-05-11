use clap::{CommandFactory, FromArgMatches, Parser};
use std::io::IsTerminal;
use tokio::signal;
use tokio::sync::oneshot;
use umadb_server::{
    start_server_secure_from_files_with_api_key_with_options,
    start_server_secure_from_files_with_options, start_server_with_api_key_with_options,
    start_server_with_options, uptime,
};


pub fn print_banner() {
    const ART: &[&str] = &[
        r"██╗  ██╗ ███╗   ███╗  █████╗  ██████╗  ██████╗ ",
        r"██║  ██║ ████╗ ████║ ██╔══██╗ ██╔══██╗ ██╔══██╗",
        r"██║  ██║ ██╔████╔██║ ███████║ ██║  ██║ ██████╔╝",
        r"██║  ██║ ██║╚██╔╝██║ ██╔══██║ ██║  ██║ ██╔══██╗",
        r"╚█████╔╝ ██║ ╚═╝ ██║ ██║  ██║ ██████╔╝ ██████╔╝",
        r" ╚════╝  ╚═╝     ╚═╝ ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ",
    ];
    const PAD: &str = "    ";
    const VERSION: &str = env!("CARGO_PKG_VERSION");

    println!();
    const COLORS: &[&str] = &[
        "\x1b[38;5;214m",
        "\x1b[38;5;214m",
        "\x1b[38;5;214m",
        "\x1b[38;5;214m",
        "\x1b[38;5;214m",
        "\x1b[38;5;220m",
    ];
    let art_width = ART[0].chars().count();
    let version_width = VERSION.chars().count() + 1;
    let offset = " ".repeat(art_width - version_width - 6 );
    if std::io::stdout().is_terminal() {
        for (line, color) in ART.iter().zip(COLORS) {
            println!("{PAD}\x1b[1m{color}{line}\x1b[0m");
        }
        println!("{PAD}\x1b[2m{offset}v{VERSION}\x1b[0m");
    } else {
        for line in ART {
            println!("{PAD}{line}");
        }
        println!("{PAD}{offset}v{VERSION}");
    }
    println!();
}


#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to bind to
    #[arg(long = "listen", env = "UMADB_LISTEN", default_value = "127.0.0.1:50051")]
    listen: String,

    /// Path to database file or directory
    #[arg(long = "db-path", env = "UMADB_DB_PATH", default_value = "./uma.db")]
    db_path: String,

    /// Path to server TLS certificate (PEM), optional
    #[arg(long = "tls-cert", env = "UMADB_TLS_CERT", required = false)]
    cert: Option<String>,

    /// Path to server TLS private key (PEM), optional
    #[arg(long = "tls-key", env = "UMADB_TLS_KEY", required = false)]
    key: Option<String>,

    /// API key for authenticating clients, optional
    #[arg(long = "api-key", env = "UMADB_API_KEY", required = false)]
    api_key: Option<String>,

    /// Read method (mmap or fileio)
    #[arg(long = "read-method", env = "UMADB_READ_METHOD", default_value = "mmap")]
    read_method: String,

    /// Page cache max pages (0 to disable)
    #[arg(long = "page-cache-max-pages", env = "UMADB_PAGE_CACHE_MAX_PAGES", default_value = "0")]
    page_cache_max_pages: usize,

    /// Page cache max size in MB (0 to disable)
    #[arg(long = "page-cache-max-mb", env = "UMADB_PAGE_CACHE_MAX_MB", default_value = "0")]
    page_cache_max_mb: usize,

    /// Zero-fill pages
    #[arg(long = "zero-fill-pages", env = "UMADB_ZERO_FILL_PAGES", default_value = "true", action = clap::ArgAction::Set)]
    zero_fill_pages: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = uptime();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        )
        .max_blocking_threads(2048)
        .enable_all()
        .build()?;

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

    let cert = args.cert;
    let key = args.key;
    let api_key = args.api_key;
    let storage_options = umadb_server::StorageOptions {
        read_method: args.read_method.parse().unwrap_or(umadb_server::ReadMethod::Mmap),
        page_cache_max_pages: args.page_cache_max_pages,
        page_cache_max_mb: args.page_cache_max_mb,
        zero_fill_pages: args.zero_fill_pages,
    };

    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = signal::ctrl_c().await;
        }
        let _ = tx.send(());
    });

    print_banner();

    match (cert, key, api_key) {
        (Some(cert), Some(key), Some(api_key)) => {
            start_server_secure_from_files_with_api_key_with_options(
                args.db_path,
                &args.listen,
                rx,
                cert,
                key,
                api_key,
                storage_options,
            )
            .await?
        }
        (Some(cert), Some(key), None) => {
            start_server_secure_from_files_with_options(
                args.db_path,
                &args.listen,
                rx,
                cert,
                key,
                storage_options,
            )
            .await?
        }
        (None, None, Some(api_key)) => {
            start_server_with_api_key_with_options(
                args.db_path,
                &args.listen,
                rx,
                api_key,
                storage_options,
            )
            .await?
        }
        (None, None, None) => {
            start_server_with_options(args.db_path, &args.listen, rx, storage_options).await?
        }
        _ => {
            eprintln!(
                "Both --tls-cert and --tls-key (or UMADB_TLS_CERT and UMADB_TLS_KEY) must be provided for TLS"
            );
            std::process::exit(2);
        }
    }

    Ok(())
}
