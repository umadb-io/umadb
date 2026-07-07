import argparse
import os
import platform
from typing import Callable, cast

import umadb


def _env_bool(name: str) -> bool | None:
    value = os.getenv(name)
    if value is None:
        return None

    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ValueError(
        f"Invalid boolean value for {name}: {value!r}. Expected one of: "
        "1/0, true/false, yes/no, on/off"
    )


def main() -> None:
    if platform.system() == "Windows":
        raise SystemExit(
            "The 'umadb' server script is not supported on Windows. "
            "Please run the server on Linux or macOS."
        )

    parser = argparse.ArgumentParser(description="UmaDB gRPC server")
    parser.add_argument("--listen", default=os.getenv("UMADB_LISTEN", "127.0.0.1:50051"))
    parser.add_argument("--db-path", default=os.getenv("UMADB_DB_PATH", "./uma.db"))
    parser.add_argument("--tls-cert", default=os.getenv("UMADB_TLS_CERT"))
    parser.add_argument("--tls-key", default=os.getenv("UMADB_TLS_KEY"))
    parser.add_argument("--api-key", default=os.getenv("UMADB_API_KEY"))
    parser.add_argument("--read-method", default=os.getenv("UMADB_READ_METHOD", "fileio"))
    parser.add_argument(
        "--page-cache-max-pages",
        type=int,
        default=int(os.getenv("UMADB_PAGE_CACHE_MAX_PAGES", "0")),
    )
    parser.add_argument(
        "--page-cache-max-mb",
        type=int,
        default=int(os.getenv("UMADB_PAGE_CACHE_MAX_MB", "0")),
    )
    parser.add_argument(
        "--zero-fill-pages",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("UMADB_ZERO_FILL_PAGES") if os.getenv("UMADB_ZERO_FILL_PAGES") is not None else True,
    )

    args = parser.parse_args()
    run_server = cast(Callable[..., None], getattr(umadb, "run_server"))
    run_server(
        listen=args.listen,
        db_path=args.db_path,
        tls_cert=args.tls_cert,
        tls_key=args.tls_key,
        api_key=args.api_key,
        read_method=args.read_method,
        page_cache_max_pages=args.page_cache_max_pages,
        page_cache_max_mb=args.page_cache_max_mb,
        zero_fill_pages=args.zero_fill_pages,
    )
