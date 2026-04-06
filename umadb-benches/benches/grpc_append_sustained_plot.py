import json
import matplotlib.pyplot as plt
from pathlib import Path
import os
import sys

USE_DOCKER = bool(os.environ.get('USE_DOCKER'))
EVENTS_PER_REQUEST = int(os.environ.get('EVENTS_PER_REQUEST', '10'))
_with_docker = "_with_docker" if USE_DOCKER else ""

def plot_sustained_throughput(json_path, output_image):
    if not Path(json_path).exists():
        print(f"Error: {json_path} not found.")
        return

    with open(json_path, 'r') as f:
        data = json.load(f)

    if not data:
        print("Error: No data in JSON file.")
        return

    writers = [d['writers'] for d in data]
    throughput = [d['throughput_events_per_sec'] for d in data]
    warmup = data[0]['warmup_secs']
    duration = data[0]['duration_secs']

    plt.figure(figsize=(10, 6))
    plt.plot(writers, throughput, marker='o', linestyle='-', linewidth=2, color='blue', label='Sustained Throughput')
    
    # Add numerical annotations
    for w, t in zip(writers, throughput):
        plt.annotate(f"{t:,.0f}", (w, t), textcoords="offset points", 
                    xytext=(0, 10), ha='center', fontsize=9, fontweight='bold')

    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel('Concurrent Writers')
    plt.ylabel('Throughput (events/sec)')
    plt.title(f'UmaDB: Sustained Append Throughput ({EVENTS_PER_REQUEST} events/req){' With Docker' if _with_docker else ''}\n(Warmup: {warmup}s, Measurement: {duration}s)')
    plt.grid(True, which='both', linestyle='--', alpha=0.5)
    plt.xticks(writers, [str(w) for w in writers])
    plt.legend()

    # Ensure images directory exists
    Path("images").mkdir(exist_ok=True)
    
    plt.tight_layout()
    plt.savefig(output_image, dpi=300)
    print(f"Plot saved to {output_image}")
    plt.close()

if __name__ == "__main__":
    json_file = f"target/grpc_append_sustained_{EVENTS_PER_REQUEST}_per_request{_with_docker}.json"
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
    
    output_file = f"images/UmaDB-append-sustained-{EVENTS_PER_REQUEST}-per-request{_with_docker.replace('_', '-')}.png"
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
        
    plot_sustained_throughput(json_file, output_file)
