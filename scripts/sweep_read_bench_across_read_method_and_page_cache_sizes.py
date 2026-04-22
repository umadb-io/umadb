import subprocess
import os
import time
import json
import statistics

# Configuration
READ_METHODS = ["mmap", "fileio"]
CACHE_SIZES = ["0", "1000", "10000"]
# Criterion FILTER can be a regex, but we just filter for one at a time to be safe or run all and parse
BENCH_COMMAND_BASE = ["cargo", "bench", "--bench", "grpc_read_bench", "--"]
# We will use a smaller set of threads for quick assessment
THREADS = ["1", "2", "4", "8"]

results = []

print("Starting UmaDB Configuration Benchmark...")
print(f"Read Methods: {READ_METHODS}")
print(f"Cache Sizes: {CACHE_SIZES}")
print(f"Threads: {THREADS}")
print("-" * 40)

for method in READ_METHODS:
    for cache_size in CACHE_SIZES:
        print(f"Running config: UMADB_READ_METHOD={method}, UMADB_PAGE_CACHE_SIZE={cache_size}")
        
        env = os.environ.copy()
        env["UMADB_READ_METHOD"] = method
        env["UMADB_PAGE_CACHE_SIZE"] = cache_size
        # Limit threads in the benchmark to speed things up
        env["MAX_THREADS"] = "256"
        
        # Run the benchmark
        # Filter for specific thread counts if possible, or just run and parse
        try:
            # We use subprocess.PIPE to capture output
            # Note: criterion output can be large. 
            process = subprocess.Popen(
                BENCH_COMMAND_BASE,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                print(f"Benchmark failed for {method}/{cache_size}")
                print(stderr)
                continue

            # Simple parsing of Criterion output (looks for 'time:' lines)
            # Example line: grpc_read_unthrottled/1    time:   [12.345 ms 12.345 ms 12.345 ms]
            # print(stdout) # Debug: print full output if needed
            for line in stdout.splitlines():
                if "time:" in line:
                    parts = line.split()
                    bench_name = parts[0]
                    # bench_name is like grpc_read_unthrottled/1
                    thread_count = bench_name.split('/')[-1]
                    
                    if thread_count not in THREADS:
                        continue
                    
                    # Extract mean time (usually the middle one in [low mean high])
                    # Line looks like: ... time: [low mean high]
                    try:
                        mean_time_str = parts[4]
                        unit = parts[5]
                        
                        # Convert to ms
                        mean_time = float(mean_time_str)
                        if unit == "s":
                            mean_time *= 1000
                        elif unit == "us" or unit == "µs":
                            mean_time /= 1000
                        elif unit == "ns":
                            mean_time /= 1000000

                        result = {
                            "method": method,
                            "cache_size": cache_size,
                            "threads": thread_count,
                            "mean_time_ms": mean_time
                        }
                        print(json.dumps(result, indent=4))
                        results.append(result)
                    except (IndexError, ValueError):
                        continue
                        
        except Exception as e:
            print(f"Error running benchmark: {e}")

# Report results
print("\nBenchmark Results (Mean Time in ms, lower is better):")
print(f"{'Method':<10} {'Cache':<10} {'Threads':<10} {'Time (ms)':<10}")
print("-" * 45)

# Sort results for better display
results.sort(key=lambda x: (int(x["threads"]), x["method"], int(x["cache_size"])))

for r in results:
    print(f"{r['method']:<10} {r['cache_size']:<10} {r['threads']:<10} {r['mean_time_ms']:<10.2f}")

# Find best config per thread count
print("\nBest Configurations:")
for t in THREADS:
    t_results = [r for r in results if r["threads"] == t]
    if t_results:
        best = min(t_results, key=lambda x: x["mean_time_ms"])
        print(f"Threads {t:>3}: Best is {best['method']} with cache {best['cache_size']} ({best['mean_time_ms']:.2f} ms)")
