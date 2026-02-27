use clap::{CommandFactory, FromArgMatches, Parser};
use tokio::signal;
use tokio::sync::oneshot;
use umadb_server::{
    start_server, start_server_secure_from_files, start_server_secure_from_files_with_api_key,
    start_server_with_api_key, uptime,
};

#[allow(dead_code)]
const BANNER_BIG: &str = r#"
  _    _                 _____  ____
 | |  | |               |  __ \|  _ \
 | |  | |_ __ ___   __ _| |  | | |_) |
 | |  | | '_ ` _ \ / _` | |  | |  _ <
 | |__| | | | | | | (_| | |__| | |_) |
  \____/|_| |_| |_|\__,_|_____/|____/ "#;

const BANNER: &str = BANNER_BIG;

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    /// Address to bind to
    #[arg(long = "listen", default_value = "127.0.0.1:50051")]
    listen: String,

    /// Path to database file or directory
    #[arg(long = "db-path", default_value = "./uma.db")]
    db_path: String,

    /// Path to server TLS certificate (PEM), optional
    #[arg(long = "tls-cert", required = false)]
    cert: Option<String>,

    /// Path to server TLS private key (PEM), optional
    #[arg(long = "tls-key", required = false)]
    key: Option<String>,

    /// API key for authenticating clients, optional
    #[arg(long = "api-key", required = false)]
    api_key: Option<String>,
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

    let cert = args.cert.or_else(|| std::env::var("UMADB_TLS_CERT").ok());
    let key = args.key.or_else(|| std::env::var("UMADB_TLS_KEY").ok());
    let api_key = args.api_key.or_else(|| std::env::var("UMADB_API_KEY").ok());

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

    // println!("{}", BANNER);
    // println!("v{}", env!("CARGO_PKG_VERSION"));
    // let mut rng = rng();
    // let banner = BANNERS.choose(&mut rng).unwrap();
    let banner = BANNER;
    println!("{}  v{}", banner, env!("CARGO_PKG_VERSION"));
    println!();

    match (cert, key, api_key) {
        (Some(cert), Some(key), Some(api_key)) => {
            start_server_secure_from_files_with_api_key(
                args.db_path,
                &args.listen,
                rx,
                cert,
                key,
                api_key,
            )
            .await?
        }
        (Some(cert), Some(key), None) => {
            start_server_secure_from_files(args.db_path, &args.listen, rx, cert, key).await?
        }
        (None, None, Some(api_key)) => {
            start_server_with_api_key(args.db_path, &args.listen, rx, api_key).await?
        }
        (None, None, None) => start_server(args.db_path, &args.listen, rx).await?,
        _ => {
            eprintln!(
                "Both --tls-cert and --tls-key (or UMADB_TLS_CERT and UMADB_TLS_KEY) must be provided for TLS"
            );
            std::process::exit(2);
        }
    }

    Ok(())
}
