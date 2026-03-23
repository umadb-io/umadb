import json
import matplotlib.pyplot as plt
from pathlib import Path
import os

# Read results from JSON file
USE_DOCKER = bool(os.environ.get('USE_DOCKER'))
_with_docker = "_with_docker" if USE_DOCKER else ""

json_path = Path("target/throughput_vs_writers.json")

if not json_path.exists():
    print(f"Error: No benchmark data found at {json_path}")
    print(f"Please run the benchmark first: make bench-append-throughput-vs-writers")
    exit(1)

with open(json_path, 'r') as f:
    results = json.load(f)

if len(results) == 0:
    print(f"Error: No results in {json_path}")
    exit(1)

# Extract data
writers = [r['writers'] for r in results]
throughputs = [r['throughput_events_per_sec'] for r in results]

# Create plot
plt.figure(figsize=(8, 5))

# Plot throughput vs writers
plt.plot(writers, throughputs, marker='o', linestyle='-', linewidth=2,
         markersize=8, color='green', label='Measured throughput')

# Add numerical annotations for throughput values
for w, t in zip(writers, throughputs):
    plt.annotate(f"{t:,.0f}", (w, t), textcoords="offset points",
                xytext=(0, 8), ha='center', fontsize=8, fontweight='bold')

plt.xscale('log', base=2)
plt.yscale('log')
plt.xlabel('Number of concurrent writers')
plt.ylabel('Total throughput (events/sec)')
plt.title(f'UmaDB: Append Throughput vs Writers{' With Docker' if _with_docker else ''}')

# Show grid lines
plt.grid(True, which='both', axis='y', alpha=0.3)
plt.grid(True, which='major', axis='x', alpha=0.3)

# Set x-axis ticks to match actual writer counts
plt.xticks(writers, [str(w) for w in writers])

plt.legend(loc='lower right', fontsize=10)

plt.tight_layout()
output_file = f"images/UmaDB-append-throughput-vs-writers-bench{_with_docker.replace('_', '-')}.png"
plt.savefig(output_file, format="png", dpi=300)
print(f"Plot saved to: {output_file}")
plt.show()
