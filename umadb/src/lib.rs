use std::io::IsTerminal;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use umadb_server::{
    DEFAULT_PAGE_SIZE, ServerTlsOptions, start_server_with_options, uptime as server_uptime,
};
pub use umadb_server::{ReadMethod, ServerOptions, StorageOptions};

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
    let offset = " ".repeat(art_width - version_width - 6);
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

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub listen_addr: String,
    pub db_path: String,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub api_key: Option<String>,
    pub read_method: ReadMethod,
    pub page_cache_max_pages: usize,
    pub page_cache_max_mb: usize,
    pub zero_fill_pages: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:50051".to_string(),
            db_path: "./uma.db".to_string(),
            tls_cert_path: None,
            tls_key_path: None,
            api_key: None,
            read_method: ReadMethod::FileIo,
            page_cache_max_pages: 0,
            page_cache_max_mb: 0,
            zero_fill_pages: true,
        }
    }
}

impl RunOptions {
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.tls_cert_path.is_some() != self.tls_key_path.is_some() {
            return Err("both tls_cert_path and tls_key_path must be provided for TLS".into());
        }
        Ok(())
    }

    pub fn to_server_options(&self) -> Result<ServerOptions, Box<dyn std::error::Error>> {
        self.validate()?;

        let tls = ServerTlsOptions::from_path_strings(
            self.tls_cert_path.clone(),
            self.tls_key_path.clone(),
        )?;

        let storage = StorageOptions::default()
            .db_path(self.db_path.clone())
            .page_size(DEFAULT_PAGE_SIZE)
            .read_method(self.read_method)
            .page_cache_max_pages(self.page_cache_max_pages)
            .page_cache_max_mb(self.page_cache_max_mb)
            .zero_fill_pages(self.zero_fill_pages);

        Ok(ServerOptions {
            listen_addr: self.listen_addr.clone(),
            tls,
            api_key: self.api_key.clone(),
            storage,
        })
    }
}

pub async fn run_async(
    opts: RunOptions,
    shutdown: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let server_options = opts.to_server_options()?;
    start_server_with_options(server_options, shutdown).await
}

pub fn spawn_shutdown_on_signal() -> (oneshot::Receiver<()>, JoinHandle<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal;
            use tokio::signal::unix::{SignalKind, signal as unix_signal};
            let mut sigterm =
                unix_signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
            tokio::select! {
                _ = signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        let _ = tx.send(());
    });

    (rx, task)
}

pub fn run_blocking(opts: RunOptions) -> Result<(), Box<dyn std::error::Error>> {
    let _ = server_uptime();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        )
        .max_blocking_threads(2048)
        .enable_all()
        .build()?;

    print_banner();

    rt.block_on(async {
        let (rx, _shutdown_task) = spawn_shutdown_on_signal();
        run_async(opts, rx).await
    })
}

pub fn build_runtime() -> Result<tokio::runtime::Runtime, Box<dyn std::error::Error>> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
        )
        .max_blocking_threads(2048)
        .enable_all()
        .build()?)
}

pub fn uptime() -> std::time::Duration {
    server_uptime()
}

#[cfg(test)]
mod tests {
    use super::RunOptions;

    #[test]
    fn tls_paths_must_be_pairwise() {
        let options = RunOptions {
            tls_cert_path: Some("cert.pem".to_string()),
            tls_key_path: None,
            ..RunOptions::default()
        };

        assert!(options.validate().is_err());
    }

    #[test]
    fn default_options_validate() {
        assert!(RunOptions::default().validate().is_ok());
    }
}
