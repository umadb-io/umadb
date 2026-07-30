use umadb::{parse_cli_options, start_server_with_cli_options};

fn main() {
    // 1. Parse CLI options
    let options = match parse_cli_options(std::env::args()) {
        Ok(options) => options,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    // 2. Hand off to the library to handle the Tokio runtime, banner, signals, and execution
    if let Err(err) = start_server_with_cli_options(options) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
