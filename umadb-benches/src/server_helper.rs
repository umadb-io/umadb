use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};
use tokio::runtime::Builder as RtBuilder;
use tokio::sync::oneshot;
use umadb_server::start_server;

pub fn use_docker() -> bool {
    std::env::var("USE_DOCKER")
        .ok()
        .map(|s| !s.is_empty())
        .unwrap_or(false)
}

pub struct BenchServerHandle {
    pub shutdown_tx: oneshot::Sender<()>,
    pub server_thread: thread::JoinHandle<()>,
    pub child: Option<Child>,
    pub use_docker: bool,
}

impl BenchServerHandle {
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        let _ = self.server_thread.join();
    }
}

pub fn start_bench_server(db_path: String, addr: String) -> BenchServerHandle {
    let port = addr.split(':').last().unwrap().parse::<u16>().unwrap();

    let use_docker = use_docker();
    let (shutdown_tx, server_thread, child) = if use_docker {
        // Start Docker container
        let (tx, rx) = oneshot::channel::<()>();

        let child = Command::new("docker")
            .args([
                "run",
                "--rm", // remove container after it exits
                "--name",
                "umadb-benchmark",
                "--publish",
                &format!("{}:50051", port),
                "--volume",
                &format!("{}:/data", db_path),
                "umadb/umadb:latest",
            ])
            .spawn()
            .expect("failed to start Docker container");

        // Thread to watch for shutdown signal
        let server_thread = thread::spawn(move || {
            let _ = rx.blocking_recv(); // wait for shutdown
            // Stop the container
            let _ = Command::new("docker")
                .args(["stop", "umadb-benchmark"])
                .status();
        });

        (tx, server_thread, Some(child))
    } else {
        let (tx, rx) = oneshot::channel::<()>();
        let db_path_clone = db_path.clone();
        let addr_clone = addr.clone();

        let server_thread = thread::spawn(move || {
            let server_threads = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let rt = RtBuilder::new_multi_thread()
                .worker_threads(server_threads)
                .max_blocking_threads(2048)
                .enable_all()
                .build()
                .expect("build tokio rt for server");
            rt.block_on(async move {
                start_server(db_path_clone, &addr_clone, rx)
                    .await
                    .expect("start server");
            });
        });

        (tx, server_thread, None)
    };

    // Wait until the server is actually accepting connections (avoid race with startup)
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpStream::connect(&addr).is_ok() {
            break;
        }
        if Instant::now() >= deadline {
            panic!("server did not start listening within timeout at {}", addr);
        }
        thread::sleep(Duration::from_millis(50));
    }

    BenchServerHandle {
        shutdown_tx,
        server_thread,
        child,
        use_docker,
    }
}
