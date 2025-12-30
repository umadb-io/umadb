import json
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import os

# Keep this in sync with benches/grpc_read_with_writers_bench.rs
TOTAL_EVENTS = 100_000
WRITER_COUNT = 4  # number of background writers running during the read bench
MAX_THREADS = int(os.environ.get('MAX_THREADS', '0')) if os.environ.get('MAX_THREADS') else None

# Thread variants you ran (match the bench). Edit if you change the bench.
all_threads = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
threads = [t for t in all_threads if MAX_THREADS is None or t <= MAX_THREADS]

x = []
mean_throughputs = []
ci_lower_throughputs = []
ci_upper_throughputs = []
# Store percentile throughputs for bands from p5 to p95
# percentile_throughputs[i] contains list of throughputs for (5+i*10) percentile
percentile_throughputs = [[] for _ in range(10)]  # 5, 15, 25, ..., 85, 95

for t in threads:
    sample_path = Path(f"target/criterion/grpc_read_4writers/{t}/new/sample.json")
    est_path = Path(f"target/criterion/grpc_read_4writers/{t}/new/estimates.json")
    if not sample_path.exists() or not est_path.exists():
        # Skip missing variants gracefully
        continue
    
    with open(sample_path, 'r') as f:
        sample_data = json.load(f)
    
    with open(est_path, 'r') as f:
        est_data = json.load(f)
    
    # Extract times and iteration counts
    times = np.array(sample_data['times'])
    iters = np.array(sample_data['iters'])
    
    # Calculate per-iteration times (nanoseconds per iteration)
    per_iter_times = times / iters
    
    # Calculate percentiles for bands from p5 to p95 (5, 15, 25, ..., 85, 95)
    percentiles = [np.percentile(per_iter_times, p) for p in range(5, 96, 10)]
    
    # Get mean from estimates
    mean_ns = est_data['mean']['point_estimate']
    mean_sec = mean_ns / 1e9
    
    # Get confidence interval bounds for mean
    ci_lower_ns = est_data['mean']['confidence_interval']['lower_bound']
    ci_upper_ns = est_data['mean']['confidence_interval']['upper_bound']
    ci_lower_sec = ci_lower_ns / 1e9
    ci_upper_sec = ci_upper_ns / 1e9
    
    events_total = TOTAL_EVENTS * t  # total across all threads for this variant
    
    # Calculate mean throughput
    mean_eps = events_total / mean_sec
    
    # Calculate CI throughputs (note: inverse relationship with time)
    # Lower time bound = upper throughput bound, upper time bound = lower throughput bound
    ci_lower_eps = events_total / ci_upper_sec
    ci_upper_eps = events_total / ci_lower_sec
    
    # Calculate throughput for each percentile (note: inverse relationship with time)
    # Lower time = higher throughput, so we reverse the percentiles
    percentile_eps = [events_total / (p_ns / 1e9) for p_ns in reversed(percentiles)]
    
    x.append(t)
    mean_throughputs.append(mean_eps)
    ci_lower_throughputs.append(ci_lower_eps)
    ci_upper_throughputs.append(ci_upper_eps)
    for i, eps in enumerate(percentile_eps):
        percentile_throughputs[i].append(eps)

# Check if any data was found
if len(x) == 0:
    print(f"Error: No benchmark data found for grpc_read_with_writers_bench")
    print(f"Expected to find data in: target/criterion/grpc_read_4writers/*/new/")
    print(f"Please run the benchmark first: make bench-read-with-writers")
    exit(1)

plt.figure(figsize=(8, 5))

# Plot percentile bands from p5 to p95 with progressive shading
# Band 0: p5-p15 (lightest)
# Band 1: p15-p25
# ...
# Band 7: p75-p85
# Band 8: p85-p95 (darkest)
# We want top bands darker, so band 8 (85-95) should be darkest

# Use a color map to create progressive shading from light to dark
base_color = 'green'
num_bands = 9

for i in range(num_bands):
    # Calculate alpha: lightest at bottom (i=0), darkest at top (i=8)
    # Use range from 0.15 to 0.6 for good visibility
    alpha = 0.0 + (i / (num_bands - 1)) * 0.45
    
    lower = percentile_throughputs[i]
    upper = percentile_throughputs[i + 1]
    
    # Calculate percentile values for labels: 5, 15, 25, ..., 85, 95
    p_lower = 5 + i * 10
    p_upper = 5 + (i + 1) * 10
    
    label = f'p{p_lower}-p{p_upper}'

    plt.fill_between(x, lower, upper, alpha=alpha, color=base_color, label=label, linewidth=0)

# Add thin black lines at p5 and p95 boundaries
plt.plot(x, percentile_throughputs[0], linestyle='-', linewidth=0.8, color='black', alpha=0.7, zorder=5)
plt.plot(x, percentile_throughputs[9], linestyle='-', linewidth=0.8, color='black', alpha=0.7, zorder=5)

# Calculate error bar values (asymmetric errors)
yerr_lower = [mean - lower for mean, lower in zip(mean_throughputs, ci_lower_throughputs)]
yerr_upper = [upper - mean for mean, upper in zip(mean_throughputs, ci_upper_throughputs)]

# Plot mean throughput with round markers in black and confidence intervals
plt.errorbar(x, mean_throughputs, yerr=[yerr_lower, yerr_upper], 
             marker='', linestyle='-', linewidth=2, markersize=4,
             label='Mean (95% CI)', color='black', capsize=3, capthick=1.5, 
             elinewidth=1.5, zorder=10)

# Add numerical annotations for mean values
for t, eps in zip(x, mean_throughputs):
    plt.annotate(f"{eps:,.0f}", (t, eps), textcoords="offset points", 
                xytext=(0, 8), ha='center', fontsize=8, fontweight='bold')

plt.xscale('log')
plt.yscale('log')
plt.xlabel('Reader clients')
plt.ylabel('Total events/sec')
plt.title(f'UmaDB: Read with {WRITER_COUNT} Concurrent Writers')
# Show y-axis grid lines and x-axis grid lines only at major ticks (the labeled x ticks)
plt.grid(True, which='both', axis='y', alpha=0.3)
plt.grid(True, which='major', axis='x', alpha=0.3)
plt.xticks(x, [str(t) for t in x])
plt.legend(loc='lower right', fontsize=8, ncol=2)

# Set y-axis minimum to the lowest plotted value rounded down to the next power of 10
min_value = min(percentile_throughputs[0])  # p5 percentile has the lowest values
# Round down to the next power of 10: 10^floor(log10(min_value))
y_min = 10 ** np.floor(np.log10(min_value))

# Set y-axis maximum to the highest plotted value rounded up to the next power of 10
# Consider both p95 percentile and confidence interval upper bounds
max_value = max(max(percentile_throughputs[9]), max(ci_upper_throughputs))
# Round up to the next power of 10: 10^ceil(log10(max_value))
y_max = 10 ** np.ceil(np.log10(max_value))
plt.ylim(bottom=y_min, top=y_max)

plt.tight_layout()
plt.savefig(f"images/UmaDB-read-with-writers-bench.png", format="png", dpi=300)
plt.show()
