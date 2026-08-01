import subprocess
import os
import sys
from pathlib import Path

# Configuration for the sweep
CONFIGS = [
    {"UMADB_PIPELINED_WRITER": "true", "UMADB_PAGE_CACHE_MAX_MB": "0"},
    {"UMADB_PIPELINED_WRITER": "true", "UMADB_PAGE_CACHE_MAX_MB": "1000"},
    {"UMADB_PIPELINED_WRITER": "false", "UMADB_PAGE_CACHE_MAX_MB": "0"},
    {"UMADB_PIPELINED_WRITER": "false", "UMADB_PAGE_CACHE_MAX_MB": "1000"},
]

# Default benchmark parameters
EVENTS_PER_REQUEST = os.environ.get("EVENTS_PER_REQUEST", "1")
MAX_THREADS = os.environ.get("MAX_THREADS", "64")
MIN_THREADS = os.environ.get("MIN_THREADS", "1")
INITIAL = os.environ.get("INITIAL", "100000")
DURATION = os.environ.get("DURATION", "15")

def run_bench(config):
    env = os.environ.copy()
    env.update(config)
    env["EVENTS_PER_REQUEST"] = EVENTS_PER_REQUEST
    env["MAX_THREADS"] = MAX_THREADS
    env["MIN_THREADS"] = MIN_THREADS
    env["INITIAL"] = INITIAL
    env["DURATION"] = DURATION
    
    print(f"Running bench with: " + ", ".join(f"{k}: {v}" for (k, v) in config.items()) + " ---")
    cmd = ["make", "bench-append-cond-1"]
    process = subprocess.Popen(cmd, env=env)
    process.wait()
    
    if process.returncode != 0:
        print(f"Benchmark failed for config {config}")
        return False
    return True

def main():
    # Ensure target directory exists
    success = True
    for config in CONFIGS:
        Path("images").mkdir(exist_ok=True)
        if run_bench(config):
            if "GITHUB_ACTIONS" in os.environ:
                Path("images").move(f"images-pw-{config['UMADB_PIPELINED_WRITER']}-cache-{config['UMADB_PAGE_CACHE_MAX_MB']}")
        else:
            success = False
            
    if success:
        print("All benchmarks completed successfully.")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
