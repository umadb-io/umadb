import sys
import platform

import umadb

def main() -> None:
    if platform.system() == "Windows":
        raise SystemExit(
            "The 'umadb' server script is not supported on Windows. "
            "Please run the server on Linux or macOS."
        )

    # sys.argv is a list where the first item is the executable name (e.g., 'umadb')
    # and the rest are the user's flags. Clap expects exactly this format.
    try:
        umadb.run_server_from_args(sys.argv)
    except umadb.ServerStartError as e:
        # Prints the exact error message from Rust to stderr and exits with code 1
        sys.exit(str(e))
    except KeyboardInterrupt:
        sys.exit(0)

__all__ = ["main"]
