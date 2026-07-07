use umadb::{parse_args_from, run_blocking};

fn main() {
    // 1. Parse arguments from the environment
    let run_options = match parse_args_from(std::env::args()) {
        Ok(opts) => opts,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    // 2. Hand off to the library to handle the Tokio runtime, banner, signals, and execution
    if let Err(err) = run_blocking(run_options) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
